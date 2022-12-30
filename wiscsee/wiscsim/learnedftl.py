from asyncore import write
import bidict
import sys
import copy
from collections import deque, OrderedDict, defaultdict
import datetime
import time
import Queue
import itertools
import math
import objgraph
# from pympler import asizeof
import pickle
from bisect import bisect_left, insort_left
import numpy as np
# from wiscsim.lsm_tree.bloom_filter import BloomFilter
import bitarray
import bitarray.util
from wiscsim.utils import *
from wiscsim.sftl import SFTLPage, DFTLPage

import config
import ftlbuilder
from datacache import *
import recorder
from utilities import utils
from .bitmap import FlashBitmap2
from wiscsim.devblockpool import *
from ftlsim_commons import *
from commons import *
import dftldes

LPN_TO_DEBUG = -1

# some constants
LPN_BYTES = 4
SUBLPN_BYTES = 4  # = 1 if use FramePLR
PPN_BYTES = 4
FLOAT16_BYTES = 2
LENGTH_BYTES = 1  # MAX_SEGMENT_LENGTH = 256

CACHE_HIT = 300
COMPACTION_DELAY = 4300
LOOKUP_LATENCY = 50

# relearn_counter = 0
# relearn_ppns = []


# FTL components

class Ftl(ftlbuilder.FtlBuilder):

    def __init__(self, confobj, recorderobj, flashobj, simpy_env, des_flash, ncq):
        super(Ftl, self).__init__(confobj, recorderobj, flashobj)

        self.des_flash = des_flash
        self.env = simpy_env
        self.ncq = ncq

        self.counter = defaultdict(float)

        self.metadata = FlashMetadata(self.conf, self.counter)
        self.logical_block_locks = LockPool(self.env)

        self.written_bytes = 0
        self.discarded_bytes = 0
        self.pre_written_bytes_gc = 0
        self.read_bytes = 0
        self.pre_written_bytes = 0
        self.pre_discarded_bytes = 0
        self.pre_read_bytes = 0
        self.display_interval = 1000 * MB
        self.compaction_interval = 100 * MB
        self.promotion_interval = 500 * MB
        self.gc_interval = 1000 * MB
        self.rw_events = 0
        #self.buffer = WriteBuffer(16*self.conf.n_pages_per_block, 16*self.conf.n_pages_per_block, filtering=7.0/8.0)
        #self.buffer = SimpleWriteBuffer(self.conf.n_pages_per_block)
        #self.datacache = DataCache(self.conf['cache_size'], self.conf.page_size, method="LRU", priority=False)
        # print(self.conf.page_size)
        self.rw_cache = RWCache(self.conf['cache_size'], self.conf.page_size, 8*MB, 7.0/8.0)

        self.hist = defaultdict(int)
        self.read_latencies = []
        self.write_latencies = []
        self.waf = {"request" : 0, "actual" : 0}
        self.raf = {"request" : 0, "actual" : 0}
        self.enable_recording = False
    


    def recorder_enabled(self, enable=True):
        self.enable_recording = enable

    def lpn_to_ppn(self, lpn):
        return self.metadata.lpn_to_ppn(lpn)

    def end_ssd(self):

        self.metadata.mapping_table.compact(promote=True)

        log_msg("End-to-end overall response time per page: %.2fus; Num of requests %d" % ((np.sum(self.write_latencies) + np.sum(self.read_latencies)) /  (self.waf["request"] + self.raf['request']), self.waf["request"] + self.raf['request']))

        if len(self.read_latencies) > 0:
            log_msg("End-to-end read response time per page: %.2fus; Num of reads %d" % (np.sum(self.read_latencies) / self.raf['request'], self.raf['request']))

        if len(self.write_latencies) > 0:
            log_msg("End-to-end write response time per page: %.2fus; Num of writes %d" % (np.sum(self.write_latencies) /  self.waf["request"], self.waf['request']))

        if self.waf['request'] > 0:
            log_msg("Write Amplification Factor: %.2f; Actual: %d; Request: %d" % (self.waf['actual'] / float(self.waf['request']), self.waf['actual'], self.waf['request']))

        if self.raf['request'] > 0:
            log_msg("Read Amplification Factor: %.2f; Actual: %d; Request: %d" % (self.raf['actual'] / float(self.raf['request']), self.raf['actual'], self.raf['request']))

        if self.counter['mapping_table_write_miss'] + self.counter['mapping_table_write_hit'] > 0:
            log_msg("Mapping Table Write Miss Ratio: %.2f" % (self.counter['mapping_table_write_miss'] / float(self.counter['mapping_table_write_miss'] + self.counter['mapping_table_write_hit'])))

        if self.counter['mapping_table_read_miss'] + self.counter['mapping_table_read_hit'] > 0:
            log_msg("Mapping Table Read Miss Ratio: %.2f" % (self.counter['mapping_table_read_miss'] / float(self.counter['mapping_table_read_miss'] + self.counter['mapping_table_read_hit'])))

        log_msg(self.counter)
        if sum(self.metadata.levels.values()) > 0:
            log_msg("Avg lookup", sum(self.metadata.levels.values()), sum(int(k)*int(v) for k, v in self.metadata.levels.items()) / float(sum(self.metadata.levels.values())))

        self.recorder.append_to_value_list('distribution of lookups',
                self.metadata.levels)

        
        crb_distribution = []
        for frame in self.metadata.mapping_table.frames.values():
            crb_size = 0
            for segment in frame.segments:
                if segment.filter:
                    crb_size += len([e for e in segment.filter if e])
            crb_distribution.append(crb_size)

        self.recorder.append_to_value_list('distribution of CRB size',
                crb_distribution)

        if len(crb_distribution) > 0:
            log_msg("CRB avg size: %.2f, CRB 99 size: %.2f, CRB variation: %.2f" % (np.average(crb_distribution), np.percentile(crb_distribution, 99), np.std(crb_distribution)))

    def display_msg(self, mode):
        # log_msg('Event', self.rw_events, 'Read (MB)', self.read_bytes / MB, 'reading', round(float(req_size) / MB, 2))
        display_bytes = 0
        if mode == "Read":
            display_bytes = self.read_bytes
        elif mode == "Write":
            display_bytes = self.written_bytes

        avg_lookup = 0
        if float(sum(self.metadata.levels.values())) != 0:
            avg_lookup = sum(int(k)*int(v) for k, v in self.metadata.levels.items()) / float(sum(self.metadata.levels.values()))
        log_msg('Event', self.rw_events, '%s (MB)' % mode, display_bytes / MB, "Mapping Table", self.metadata.mapping_table.memory, "Reference Mapping Table", self.metadata.reference_mapping_table.memory, "Distribution of lookups", self.metadata.levels[1], sum(self.metadata.levels.values()), avg_lookup, "Misprediction", self.hist, "Latency per page: %.2fus" % ((np.sum(self.write_latencies) + np.sum(self.read_latencies)) /  (self.waf["request"] + self.raf['request'])))
        sys.stdout.flush()

        # if float(self.waf['request']) > 0:
        #     log_msg("Write Amplification Factor: %.2f; Actual: %d; Request: %d" % (self.waf['actual'] / float(self.waf['request']), self.waf['actual'], self.waf['request']))

        # if float(self.raf['request']) > 0:
        #     log_msg("Read Amplification Factor: %.2f; Actual: %d; Request: %d" % (self.raf['actual'] / float(self.raf['request']), self.raf['actual'], self.raf['request']))

        # if float(self.counter['mapping_table_write_miss'] + self.counter['mapping_table_write_hit']) > 0:
        #     log_msg("Mapping Table Write Miss Ratio: %.2f" % (self.counter['mapping_table_write_miss'] / float(self.counter['mapping_table_write_miss'] + self.counter['mapping_table_write_hit'])))

        # if float(self.counter['mapping_table_read_miss'] + self.counter['mapping_table_read_hit']) > 0:
        #     log_msg("Mapping Table Read Miss Ratio: %.2f" % (self.counter['mapping_table_read_miss'] / float(self.counter['mapping_table_read_miss'] + self.counter['mapping_table_read_hit'])))

    def read_ext(self, extent):
        should_print = False
        req_size = extent.lpn_count * self.conf.page_size
        self.recorder.add_to_general_accumulater('traffic', 'read', req_size)
        self.read_bytes += req_size
        self.rw_events += 1
        if self.read_bytes > self.pre_read_bytes + self.display_interval:
            self.display_msg("Read")
            self.pre_read_bytes = self.read_bytes

        extents = split_ext(extent)
        start_time = self.env.now # <----- start
            
        op_id = self.recorder.get_unique_num()
        # if op_id == 279353:
        #     should_print = True
        procs = []
        total_pages_to_read = []
        total_pages_to_write = []
        requested_read = 0.0

        if should_print:
            log_msg(start_time)

        for ext in extents:
             # nothing to lookup if no lpn is written
            if not self.metadata.reference_mapping_table.get(ext.lpn_start):
                self.counter['RAW'] += 1
                continue

            assert(ext.lpn_count == 1)
            requested_read += 1
            #cachehit, writeback, lpn_previous = self.datacache.read(ext.lpn_start)
            
            # cachehit = self.datacache.read(ext.lpn_start) # or self.buffer.read(ext.lpn_start)
            cachehit, writeback, ppn = self.rw_cache.read(ext.lpn_start)


            if not cachehit:
                procs += [self.env.process(self.read_logical_block(ext, should_print))]
            else:
                self.hist[0] += 1
                yield self.env.timeout(CACHE_HIT)
                # yield self.env.timeout(delay=100)
            # if writeback:
                # additional_lpns_to_evict = self.datacache.evict_extra(0)
                # yield self.env.process(self.write_ext(Extent(lpn_previous, 1), cached=False))
                # yield extra writes
            
            if writeback:
                yield self.env.process(self._write_ppns([ppn]))
                total_pages_to_write.append(ppn)

        # self.env.exit(ext_data)
        if should_print:
            log_msg(requested_read)
            log_msg(self.env.active_process)

        ret = yield simpy.AllOf(self.env, procs)

        total_pages_to_read += [page for pages in ret.values() for page in pages[1]]
        total_pages_to_write += [page for pages in ret.values() for page in pages[0]]

        lpns_to_read = float(len(total_pages_to_read))
    
        end_time = self.env.now

        if self.enable_recording:
            if requested_read > 0:
                self.read_latencies += [(end_time - start_time) / 1000.0] # [(end_time - start_time)/(1000.0*requested_read)]*int(requested_read)

                self.raf["request"] += requested_read
                self.raf["actual"] += lpns_to_read
                self.waf["actual"] += len(total_pages_to_write)

                write_timeline(self.conf, self.recorder,
                    op_id = op_id, op = 'read_ext', arg = extent.lpn_count,
                    start_time = start_time, end_time = end_time)

        # if lpns_to_read > len(extents) and len(extents) >= 6:
        #     log_msg(lpns_to_read, len(extents), requested_read, len(pages_to_write), ((end_time - start_time)/(1000.0*requested_read)), end_time - start_time)
        #     log_msg([page // self.conf.n_pages_per_channel for page in pages_to_read])

        #self.env.exit(lpns_to_read)

    def read_logical_block(self, extent, should_print=False):
        assert(extent.lpn_count == 1)

        # replace the following lines with a nice interface
        lpn = extent.lpn_start
        read_ppns = []

        # if self.conf["dry_run"]:
        #     self.hist[len(read_ppns)] += 1
        #     read_ppn = self.metadata.reference_mapping_table.get(lpn)
        #     yield self.env.process(self.des_flash.rw_ppns([read_ppn], 'read', tag = "Unknown"))
        #     return

        ppn, pages_to_write, pages_to_read = self.metadata.lpn_to_ppn(lpn)

        if ppn:
            # if accurate mapping entry
            if ppn not in pages_to_read:
                pages_to_read.append(ppn)

            self.hist[len(pages_to_read)] += 1
            # if len(read_ppns) >= 2:
            #     self.datacache.set_priority(extent.lpn_start, priority=True)
            # read_ppns = read_ppns[-1:]
            block_id = ppn / self.conf.n_pages_per_block


            #content = self.flash.page_read(ppn, cat = TAG_FORGROUND)
            #contents.append(content)
            procs = []
            for read_ppn in pages_to_read:
                if should_print:
                    log_msg(read_ppn, self.env.now)
                yield self.env.process(self._read_ppns([read_ppn]))

            for write_ppn in pages_to_write:
                if should_print:
                    log_msg(write_ppn, self.env.now)
                yield self.env.process(self._write_ppns([write_ppn]))

            # yield simpy.AllOf(self.env, procs)
            if should_print:
                log_msg("Hi", pages_to_read, self.env.now)

        # else:
        #     # yield self.env.timeout(delay=self.conf['flash_config']['t_R'])
        #     yield self.env.process(
        #         self.des_flash.rw_ppns([0], 'read', tag = "Unknown"))
        self.env.exit((pages_to_write, pages_to_read))

    def lba_write(self, lpn, data=None):
        yield self.env.process(
            self.write_ext(Extent(lpn_start=lpn, lpn_count=1), [data]))

        # yield self.env.process(self.garbage_collector.clean())

    def write_ext(self, extent, data=None):
        req_size = extent.lpn_count * self.conf.page_size
        self.recorder.add_to_general_accumulater('traffic', 'write', req_size)
        self.written_bytes += req_size
        self.rw_events += 1
        
        if self.written_bytes > self.pre_written_bytes + self.display_interval:
            self.metadata.mapping_table.compact()
            self.metadata.mapping_table.promote()
            self.display_msg("Write")
            self.pre_written_bytes = self.written_bytes

            yield self.env.timeout(COMPACTION_DELAY)
            # roots = objgraph.get_leaking_objects()
            # objgraph.show_most_common_types() 
            # objgraph.show_growth() 

            # print(sys.getsizeof(self.metadata))
            # print(sys.getsizeof(self.metadata.mapping_table))

            # print(len(pickle.dumps(self.datacache)))
            # print(len(pickle.dumps(self.metadata)))
            # print(len(pickle.dumps(self.metadata.mapping_table)))

            # from pympler import asizeof
            # print(asizeof.asizeof(self.metadata))
            # print(asizeof.asizeof(self.metadata.reference_mapping_table))
            # print(asizeof.asizeof(self.metadata.pvb))
            # print(asizeof.asizeof(self.metadata.bvc))
            # print(asizeof.asizeof(self.metadata.oob))
            # print(asizeof.asizeof(self.metadata.levels))
            # print(asizeof.asizeof(self.metadata.mapping_table.frames))
            # print("")
            # # print(vars(self.metadata.mapping_table))
            # print(asizeof.asizeof(self.metadata.mapping_table.frames.keys()))
            # print(asizeof.asizeof(self.metadata.mapping_table.frames.values()))
            # print(asizeof.asizeof(self.metadata.mapping_table.GTD))
            # print(asizeof.asizeof(self.metadata.mapping_table.memory_counter))
            # print(asizeof.asizeof(self.metadata.mapping_table.frame_on_flash))
            # print(asizeof.asizeof(self.metadata.mapping_table.dirty))
            


            # print(asizeof.asizeof(self.metadata.mapping_table.frames.values()[0].runs))
            # print(asizeof.asizeof(self.metadata.mapping_table.frames.values()[0].segments))

            # log_msg('Mapping Table Compaction Begins')
        
       # if self.written_bytes % self.compaction_interval == 0:
            self.metadata.mapping_table.compact()

       # if self.written_bytes % self.promotion_interval == 0:
            self.metadata.mapping_table.promote()
            # log_msg('Mapping Table Compaction Ends')

        extents = split_ext(extent)
        start_time = self.env.now # <----- start
        op_id = self.recorder.get_unique_num()
        write_procs = []
        total_pages_to_write = []
        for ext in extents:
            # if cached:
            #     assert(ext.lpn_count == 1)
            #     writeback, lpn_previous = self.datacache.write(ext.lpn_start)
            #     if writeback:
            # additional_lpns_to_evict = self.datacache.evict_extra(0)
            # yield self.env.process(self.write_ext(Extent(lpn_previous, 1), cached=False))
            # yield extra writes
            # else:
                #self.datacache.resize(4*MB - self.metadata.mapping_table.memory)
                #self.datacache.resize(4*MB - self.metadata.reference_mapping_table.memory)
                # for lpn in exts:
                #     p = self.env.process(self.write_single_page(mappings[lpn]))
                #     write_procs.append(p)
            # print(ext.lpn_start)
            # if self.rw_events > 10:
            #     exit()

            # TODO: should we modify data cache here? Jinghan: Done
            # FIXME: check if there is any bug here
            # self.datacache.read(ext.lpn_start)
            writeback, ppn = self.rw_cache.write(ext.lpn_start)
            if writeback:
                p = self.env.process(self._write_ppns([ppn]))
                write_procs.append(p)
                total_pages_to_write.append(ppn)

            
            # self.datacache.invalidate(ext)
            if self.rw_cache.should_assign_page():
                exts = self.rw_cache.flush_unassigned()
                mappings, pages_to_read, pages_to_write = self.metadata.update(exts)
                self.rw_cache.update_assigned(mappings)
                total_pages_to_write.extend(pages_to_write)
                # if len(pages_to_read) != 0 or len(pages_to_write) != 0:
                #     print(pages_to_read, pages_to_write)

            # self.buffer.write(ext.lpn_start)
            # self.datacache.invalidate(ext)
            # if self.buffer.should_flush():
            #     exts = self.buffer.flush()
            #     self.counter += len(exts)
            #     mappings = self.metadata.update(exts)
                for ppn in pages_to_write:
                    p = self.env.process(self._write_ppns([ppn]))
                    write_procs.append(p)

                for ppn in pages_to_read:
                    p = self.env.process(self._read_ppns([ppn]))
                    write_procs.append(p)

            if not writeback:
                yield self.env.timeout(CACHE_HIT)

        yield simpy.AllOf(self.env, write_procs)

        end_time = self.env.now # <----- end

        if self.enable_recording:
            self.write_latencies.append((end_time - start_time)/1000.0)
            self.waf["request"] += extent.lpn_count
            self.waf["actual"] += len(total_pages_to_write)
        

        write_timeline(self.conf, self.recorder,
            op_id = op_id, op = 'write_ext', arg = extent.lpn_count,
            start_time = start_time, end_time = end_time)

    def _block_iter_of_extent(self, extent):
        block_start, _ = self.conf.page_to_block_off(extent.lpn_start)
        block_last, _ = self.conf.page_to_block_off(extent.last_lpn())

        return range(block_start, block_last + 1)

    def print_mappings(self, mappings):
        block = 1136
        for lpn, ppn in mappings.items():
            blk, _ = self.conf.page_to_block_off(lpn)
            # blk, _ = self.conf.page_to_block_off(ppn)
            if blk == block:
                print(lpn, '->', ppn)

    def _sub_ext_data(self, data, extent, sub_ext):
        start = sub_ext.lpn_start - extent.lpn_start
        count = sub_ext.lpn_count
        sub_data = data[start:(start + count)]
        return sub_data

    def _write_ppns(self, ppns):
        """
        The ppns in mappings is obtained from loggroup.next_ppns()
        """
        # flash controller
        yield self.env.process(
            self.des_flash.rw_ppns(ppns, 'write',
                                   tag="Unknown"))

        self.env.exit((0, 0))

    def _read_ppns(self, ppns):
        """
        The ppns in mappings is obtained from loggroup.next_ppns()
        """
        # flash controller
        yield self.env.process(
            self.des_flash.rw_ppns(ppns, 'read',
                                   tag="Unknown"))

    def _update_log_mappings(self, mappings):
        """
        The ppns in mappings must have been get by loggroup.next_ppns()
        """
        for lpn, ppn in mappings.items():
            self.log_mapping_table.add_mapping(lpn, ppn)

    def _remap_oob(self, new_mappings):
        for lpn, new_ppn in new_mappings.items():
            found, old_ppn, loc = self.translator.lpn_to_ppn(lpn)
            if found == False:
                old_ppn = None
            self.oob.remap(lpn=lpn, old_ppn=old_ppn, new_ppn=new_ppn)

    def lba_discard(self, lpn):
        yield self.env.process(self._discard_logical_block(Extent(lpn, 1)))

    def discard_ext(self, extent):
        req_size = extent.lpn_count * self.conf.page_size
        self.recorder.add_to_general_accumulater(
            'traffic', 'discard', req_size)
        self.discarded_bytes += req_size
        if self.discarded_bytes > self.pre_discarded_bytes + self.display_interval:
            log_msg('Discarded (MB)', self.pre_discarded_bytes / MB, 'discarding', round(float(req_size) / MB, 2))
            sys.stdout.flush()
            self.pre_discarded_bytes = self.discarded_bytes

        self.recorder.add_to_general_accumulater('traffic', 'discard',
                                                 extent.lpn_count*self.conf.page_size)

        extents = split_ext(self.conf.n_pages_per_block, extent)
        for logical_block_ext in extents:
            yield self.env.process(self._discard_logical_block(logical_block_ext))

    def _discard_logical_block(self, extent):
        block_id, _ = self.conf.page_to_block_off(extent.lpn_start)
        req = self.logical_block_locks.get_request(block_id)
        yield req

        for lpn in extent.lpn_iter():
            found, ppn, loc = self.translator.lpn_to_ppn(lpn)
            if found == True:
                if loc == IN_LOG_BLOCK:
                    self.translator.log_mapping_table.remove_lpn(lpn)
                self.oob.wipe_ppn(ppn)

        self.logical_block_locks.release_request(block_id, req)

    def post_processing(self):
        pass

    def clean(self, forced=False, merge=True):

        self.pre_written_bytes_gc = self.written_bytes
        erased_pbns = []
        validate_pages = []
  
        num_valid = sum(self.metadata.bvc.counter[block] for block in self.metadata.bvc.counter)
        num_all = float(sum(self.conf.n_pages_per_block for block in self.metadata.bvc.counter if self.metadata.bvc.counter[block] > 0))
        # print(num_valid / num_all)
        if num_all == 0:
            return
        if num_valid / num_all > 0.9:
            return

        for block in self.metadata.bvc.counter:
            num_validate_pages = self.metadata.bvc.counter[block]
            if num_validate_pages / 256.0 <= 0.1:
                erased_pbns.append(block)
                # print(block)
                validate_pages += self.metadata.pvb.get_valid_pages(block)

        # print(validate_pages)
        count = 0
        all_ppns_to_write = []
        for lpn in validate_pages:
            if count % 256 == 0:
                next_free_block = self.metadata.bvc.next_free_block()
                self.metadata.pvb.validate_block(next_free_block)
                next_free_ppn = self.conf.n_pages_per_block * next_free_block
                count = 1
            else:
                next_free_ppn += 1
                count += 1
            all_ppns_to_write.append(next_free_ppn)

        # print(all_ppns_to_write)

        erase_procs = []
        for erased_pbn in erased_pbns:
            erase_procs += [self.env.process(self.des_flash.erase_pbn_extent(pbn_start = erased_pbn, pbn_count = 1, tag = None))]

        write_procs = []
        for ppn in all_ppns_to_write:
            p = self.env.process(self._write_ppns([ppn]))
            write_procs.append(p)
            
        start = self.env.now
        yield simpy.AllOf(self.env, erase_procs)
        erase_finished = self.env.now
        yield simpy.AllOf(self.env, write_procs)
        write_finished = self.env.now
        print(len(validate_pages), erase_finished - start, write_finished - erase_finished)

    def is_wear_leveling_needed(self):
        factor, diff = self.block_pool.get_wear_status()
        self.recorder.append_to_value_list('wear_diff', diff)
        # print 'ddddddddddddddddddddiiiiiiiiiiifffffffffff', diff

        return self.block_pool.need_wear_leveling()

    def level_wear(self):
        pass

    def is_cleaning_needed(self):
        if self.written_bytes > self.pre_written_bytes_gc + self.gc_interval:
            return True
        return False

    def snapshot_erasure_count_dist(self):
        dist = self.block_pool.get_erasure_count_dist()
        # print self.env.now
        # print dist
        self.recorder.append_to_value_list('ftl_func_erasure_count_dist',
                                           dist)

    def snapshot_user_traffic(self):
        return
        self.recorder.append_to_value_list('ftl_func_user_traffic',
                {'timestamp': self.env.now/float(SEC),
                 'write_traffic_size': self.written_bytes,
                 'read_traffic_size': self.read_bytes,
                 'discard_traffic_size': self.discarded_bytes,
                 },
                )

class PageValidityBitmap(object):
    "Using one bit to represent state of a page"
    "Erased state is recorded by BVC"
    VALID, INVALID = (1, 0)

    def __init__(self, conf, bvc):
        if not isinstance(conf, config.Config):
            raise TypeError("conf is not conf.Config. it is {}".
                            format(type(conf).__name__))

        self.conf = conf
        self.bitmap = bitarray.bitarray(conf.total_num_pages())
        self.bitmap.setall(0)
        self.bvc = bvc

    def validate_page(self, pagenum):
        self.bitmap[pagenum] = self.VALID
        self.bvc.counter[pagenum // self.conf.n_pages_per_block] += 1

    def invalidate_page(self, pagenum):
        self.bitmap[pagenum] = self.INVALID
        self.bvc.counter[pagenum // self.conf.n_pages_per_block] -= 1

    def validate_block(self, blocknum):
        ppn_start, ppn_end = self.conf.block_to_page_range(blocknum)
        for pg in range(ppn_start, ppn_end):
            self.validate_page(pg)

    def invalidate_block(self, blocknum):
        ppn_start, ppn_end = self.conf.block_to_page_range(blocknum)
        for pg in range(ppn_start, ppn_end):
            self.invalidate_page(pg)

    def get_num_valid_pages(self, blocknum):
        start, end = self.conf.block_to_page_range(blocknum)
        return sum(self.bitmap[start:end])

    def get_valid_pages(self, blocknum):
        valid_pages = []
        start, end = self.conf.block_to_page_range(blocknum)
        for pg in range(start, end):
            if self.bitmap[pg]:
                valid_pages.append(pg)
        return valid_pages


    def is_page_valid(self, pagenum):
        return self.bitmap[pagenum] == self.VALID

    def is_page_invalid(self, pagenum):
        return self.bitmap[pagenum] == self.INVALID

    @property
    def memory(self):
        return round(len(self.bitmap) // 8)


class BlockValidityCounter(object):
    """
        Timestamp table PPN -> timestamp
        Here are the rules:
        1. only programming a PPN updates the timestamp of PPN
           if the content is new from FS, timestamp is the timestamp of the
           LPN
           if the content is copied from other flash block, timestamp is the
           same as the previous ppn
        2. discarding, and reading a ppn does not change it.
        3. erasing a block will remove all the timestamps of the block
        4. so cur_timestamp can only be advanced by LBA operations
        Table PPN -> valid pages
    """

    def __init__(self, conf):
        self.conf = conf
        self.last_inv_time_of_block = {}
        self.timestamp_table = {}
        self.cur_timestamp = 0
        self.counter = defaultdict(lambda:0)
        self.free_block_list = [self.conf.n_blocks_per_channel * channel + block 
                                for block in range(self.conf.n_blocks_per_channel)
                                for channel in range(self.conf.n_channels_per_dev)]

    def _incr_timestamp(self):
        """
        This function will advance timestamp
        """
        t = self.cur_timestamp
        self.cur_timestamp += 1
        return t

    def set_timestamp_of_ppn(self, ppn):
        self.timestamp_table[ppn] = self._incr_timestamp()

    def copy_timestamp(self, src_ppn, dst_ppn):
        self.timestamp_table[dst_ppn] = self.timestamp_table[src_ppn]

    def get_num_valid_pages(self, blocknum):
        return self.counter[blocknum]

    # FIXME improve efficiency; now round-robin
    # bottleneck
    def next_free_block(self, wear_level=False):
        free_block = self.free_block_list.pop(0)
        return free_block
        # for block in range(self.conf.n_blocks_per_channel):
        #     for channel in range(self.conf.n_channels_per_dev):
        #         pbn = self.conf.n_blocks_per_channel * channel + block
        #         if self.get_num_valid_pages(pbn) == 0:
        #             yield pbn

    def gc_block(self, blocknum):
        self.counter[blocknum] = 0
        self.free_block_list.append(blocknum)

    def memory(self):
        return


class OutOfBandAreas(object):
    """
    Jinghan: We use OOB to store the p2l mapping for each page and all PPNs within the same segment. Since we do not update the segments in-place, we also do not have to update the OOB data.
    OOB impl: Dict<ppn, Dict<ppn, lpn>>
    Real OOB: List<lpn>
    """

    def __init__(self, conf, gamma):
        self.oob_data = defaultdict(None)
        self.gamma = gamma
        self.per_page_size = conf['flash_config']["oob_size_per_page"]
        self.p2l_entry_size = 4
        self.num_p2l_entries = min(2*self.gamma, self.per_page_size / 4)

    # entries: List<Tuple<lpn, ppn>>
    def set_oob(self, source_page, entries):
        assert(entries == None or isinstance(entries, list))
        if entries == None:
            self.oob_data[source_page] = entries
        else:
            rev_map = dict() #bidict.bidict()
            for (lpn, ppn) in entries:
                rev_map[lpn] = ppn
            self.oob_data[source_page] = rev_map

    def ppn_to_lpn(self, ppn, source_page=None):
        raise NotImplementedError
        if not source_page:
            source_page = ppn
        assert(
            source_page in self.oob_data and ppn in self.oob_data[source_page])
        return self.oob_data[source_page][ppn]

    def lpn_to_ppn(self, lpn, source_page):
        if source_page in self.oob_data:
            rev_map = self.oob_data[source_page]
            if rev_map:
                return rev_map.get(lpn) #.inv.get(lpn)
        return None



class OutOfBandAreasMemOpt(object):
    """
    Memory optimized version
    Jinghan: We use OOB to store the p2l mapping for each page and all PPNs within the same segment. Since we do not update the segments in-place, we also do not have to update the OOB data.
    OOB impl: Dict<ppn, Dict<ppn, lpn>>
    Real OOB: List<lpn>
    """

    def __init__(self, conf, gamma, reference_mapping_table):
        self.gamma = gamma
        self.num_p2l_entries = min(2*self.gamma, conf['flash_config']["oob_size_per_page"] / 4)
        self.reference_mapping_table = reference_mapping_table

    # entries: List<Tuple<lpn, ppn>>
    def set_oob(self, source_page, entries):
        pass

    def ppn_to_lpn(self, ppn, source_page=None):
        pass

    def lpn_to_ppn(self, lpn, source_page):
        real_ppn = self.reference_mapping_table.get(lpn)
        if abs(real_ppn - source_page) <= self.num_p2l_entries:
            return real_ppn
        return None



class FlashMetadata(object):
    def __init__(self, confobj, counter):
        self.conf = confobj
        self.counter = counter

        self.flash_num_blocks = confobj.n_blocks_per_dev
        self.flash_npage_per_block = confobj.n_pages_per_block
        self.total_pages = self.flash_num_blocks * self.flash_npage_per_block

        # mapping table
        self.gamma = self.conf['gamma']
        #self.mapping_table = LogPLR(frame_no=0, gamma=self.gamma)
        self.mapping_table = FrameLogPLR(confobj, self, counter, gamma=self.gamma)

        Segment.PAGE_PER_BLOCK = self.flash_npage_per_block
        self.reference_mapping_table = PFTL()
        # flash block -> last invalidation time and num of valid pages
        self.bvc = BlockValidityCounter(confobj)
        # Key metadata structures
        self.pvb = PageValidityBitmap(confobj, self.bvc)
        # ppn -> lpn mapping stored in OOB
        # self.oob = OutOfBandAreas(confobj, gamma=self.gamma)
        self.oob = OutOfBandAreasMemOpt(confobj, self.gamma, self.reference_mapping_table)
        self.last_oob_page = []
        self.next_free_ppn = 0
        # WAF
        self.waf = []

        # counters
        self.levels = defaultdict(int)


    ############# Flash read related ############

    def ppn_to_lpn(self, ppn, source_page=None):
        return self.oob.ppn_to_lpn(ppn, source_page=source_page)

    ############# Flash write related ############

    def lpn_to_ppn(self, lpn):
        real_ppn = None
        results, num_lookup, pages_to_write, pages_to_read = self.mapping_table.lookup(lpn, first=True)
        self.levels[num_lookup] += 1
        # if len(results) == 0:
        #     return None, None
        if len(results) > 0:
            ppn, accurate, seg = results[0]
            if accurate:
                # pages_to_read += [ppn]
                real_ppn = ppn
                    
            else:
                actual = self.oob.lpn_to_ppn(lpn, source_page=ppn)
                if actual:
                    real_ppn = actual
                    oob_cached = False
                    for oob_page in self.last_oob_page:
                        if self.oob.lpn_to_ppn(lpn, source_page=oob_page):
                            oob_cached = True
                            break
                    if oob_cached:
                        pages_to_read += [actual]
                    else:
                        if actual == ppn:
                            pages_to_read += [ppn]
                        else:
                            pages_to_read += [ppn, actual]
                    if len(self.last_oob_page) >= 16:
                        self.last_oob_page.pop(0)
                    self.last_oob_page.append(ppn)
                # entry not exists; continue to search neighbor block (ppn is predicted to the wrong block)
                else:
                    pages_to_read += [ppn]
                    if ppn % self.conf.n_pages_per_block < self.conf.n_pages_per_block / 2.0:
                        ppn = int(ppn / self.conf.n_pages_per_block) * self.conf.n_pages_per_block - 1
                    else:
                        ppn = int(ppn / self.conf.n_pages_per_block + 1) * self.conf.n_pages_per_block
                    actual = self.oob.lpn_to_ppn(lpn, source_page=ppn)
                    try:
                        assert(actual)
                    except:
                        # if this assert fails, it is possible that prediction is out of oob range, but still in the same block
                        self.validation(lpn, None)
                    real_ppn = actual
                    if actual == ppn:
                        pages_to_read += [ppn]
                    else:
                        pages_to_read += [ppn, actual]

            self.validation(lpn, real_ppn)
        del results
        return real_ppn, pages_to_write, pages_to_read


    '''
        @return Dict<lpn, ppn>
    '''
    def update(self, extents):
        mappings = dict()
        pages_to_read = []
        pages_to_write = []
        for i in range(0, len(extents), self.conf.n_pages_per_block):
            submap, subpages_to_read, subpages_to_write = self.update_block(extents[i:i+self.conf.n_pages_per_block])
            mappings.update(submap)
            pages_to_read.extend(subpages_to_read)
            pages_to_write.extend(subpages_to_write)


        return mappings, list(set(pages_to_read)), list(set(pages_to_write))

    '''
        @return Dict<lpn, ppn>
    '''
    def update_block(self, extents):
        assert(len(extents) <= self.conf.n_pages_per_block)
        entries = []
        pages_to_read = []
        pages_to_write = []
        extents = sorted(extents)

        # find the next free block
        next_free_block = self.bvc.next_free_block()
        next_free_ppn = self.conf.n_pages_per_block * next_free_block

        # allocate flash pages
        for i, lpn in enumerate(extents):
            entry = (lpn, next_free_ppn + i)
            entries.append(entry)

        #TODO: additional flash reads; make this async; write to bitmap
        self.pvb.validate_block(next_free_block)
        # if self.conf["dry_run"]:
        #     for (lpn, ppn) in entries:
        #         self.reference_mapping_table.set(lpn, ppn)
        #     return dict(entries)

        for lpn in extents:
            old_ppn = None
            overheads = 0
            if not self.reference_mapping_table.get(lpn):
                continue

            old_ppn, pages_to_write, pages_to_read = self.lpn_to_ppn(lpn)

            if old_ppn:
                self.pvb.invalidate_page(old_ppn)

        ## mapping table gc (replaced with compaction)
        # self.mapping_table.gc(next_free_block)

        ## update mapping table
        mapping_pages_to_write, mapping_pages_to_read = self.mapping_table.update(entries, next_free_block)
        pages_to_write += mapping_pages_to_write
        pages_to_read += mapping_pages_to_read

        # update reference mapping table
        for (lpn, ppn) in entries:
            self.reference_mapping_table.set(lpn, ppn)

        # update oob
        # store the lpn of each ppn within [ppn - gamma - 1, ppn + gamma + 1]
        max_gamma = 16
        for i, (lpn, ppn) in enumerate(entries):
            # print(max(0, i - self.gamma - 1), i + self.gamma + 2)
            upper = int(i + max_gamma + 2)
            lower = int(i - max_gamma - 1)
            self.oob.set_oob(ppn, entries[max(0,lower):upper])

        return dict(entries), pages_to_read, pages_to_write


    ############# GC related ############

    def erase_block(self, flash_block):
        self.bvc.erase_block(flash_block)

        start, end = self.conf.block_to_page_range(flash_block)
        for ppn in range(start, end):
            try:
                self.oob.set_oob(ppn, None)
                # if you try to erase translation block here, it may fail,
                # but it is expected.
                del self.bvc.timestamp_table[ppn]
            except KeyError:
                pass

        try:
            del self.bvc.last_inv_time_of_block[flash_block]
        except KeyError:
            pass

    def relocate_page(self, virtual_pn, old_ppn, new_ppn, update_time=True):
        """
        mark the new_ppn as valid
        update the virtual page number in new page's OOB to virtual_pn
        invalidate the old_ppn, so cleaner can GC it
        """
        if update_time is True:
            self.bvc.set_timestamp_of_ppn(new_ppn)
        else:
            self.bvc.copy_timestamp(old_ppn, new_ppn)

        self.pvb.validate_page(new_ppn)
        self.oob.set_lpn(new_ppn, virtual_pn)

        if old_ppn != UNINITIATED:
            self.invalidate_ppn(old_ppn)

    def data_page_move(self, lpn, old_ppn, new_ppn):
        # move data page does not change the content's timestamp, so
        # we copy
        self.bvc.copy_timestamp(src_ppn=old_ppn, dst_ppn=new_ppn)
        self.relocate_page(lpn, old_ppn, new_ppn, update_time=False)

    def validation(self, lpn, ppn):
        try:
            assert(ppn == self.reference_mapping_table.get(lpn))
        except:
            # print(self.mapping_table.runs[0])
            results, num_lookup, pages_to_write, pages_to_read = self.mapping_table.lookup(lpn, first=False)
            print("lpn:", lpn)
            print("reference ppn:",
                    self.reference_mapping_table.get_all(lpn))
            print("learned ppn:", ppn)
            print("all ppns in the tree:", results)
            for ppn, accurate, seg in results:
                if seg:
                    print("learned segment:", seg.full_str())
                print("oob data:", str(self.oob.oob_data[ppn]))
            exit(0)

# Learning-related components

class SimpleSegment():
    def __init__(self, k, b, x1, x2):
        self.b = b
        self.k = k
        self.x1 = x1
        self.x2 = x2

    def __str__(self):
        return "(%d, %.2f, %d, %d)" % (self.b, self.k, self.x1, self.x2)

    def __repr__(self):
        return str(self)

    def get_y(self, x):
        predict = int(round(x*self.k + self.b))
        return predict

    @staticmethod
    def intersection(s1, s2):
        p = (float(s2.b - s1.b) / (s1.k - s2.k),
             float(s1.k * s2.b - s2.k * s1.b) / (s1.k - s2.k))
        return p

    @staticmethod
    def is_above(pt, s):
        return pt[1] > s.k * pt[0] + s.b

    @staticmethod
    def is_below(pt, s):
        return pt[1] < s.k * pt[0] + s.b

    @staticmethod
    def get_upper_bound(pt, gamma):
        return (pt[0], pt[1] + gamma)

    @staticmethod
    def get_lower_bound(pt, gamma):
        return (pt[0], pt[1] - gamma)

    @staticmethod
    def frompoints(p1, p2):
        k = float(p2[1] - p1[1]) / (p2[0] - p1[0])
        b = -k * p1[0] + p1[1]
        return SimpleSegment(k, b, p1[0], p2[0])


class Segment():
    FPR = 0.01
    PAGE_PER_BLOCK = 256
    BITMAP = True

    def __init__(self, k, b, x1, x2, points=None):
        self.b = b
        self.k = k
        self.x1 = x1
        self.x2 = x2
        self.accurate = True
        self.filter = None

        if points:
            self._points = points  # set of points only for verification purpose
            self.accurate, consecutive = self.check_properties(points)

            if not consecutive:
                if Segment.BITMAP:
                    self.filter = bitarray.bitarray(self.x2 - self.x1 + 1) 
                    self.filter.setall(0)
                    for pt in points:
                        self.filter[pt[0] - self.x1] = 1
                else:
                    self.filter = BloomFilter(len(points), Segment.FPR)
                    for pt in points:
                        self.filter.add(pt[0])
        
        if LPN_TO_DEBUG in zip(*points)[0]:
            log_msg("new seg", self)

    def __str__(self):
        return "%.4f, %d, [%d, %d], memory: %dB, accuracy: %s, bitmap: %s" \
            % (self.k, self.b, self.x1, self.x2, self.memory, self.accurate, self.filter)

    def __repr__(self):
        return str(self)
        return "(%d, %.4f, %d, %d, %s)" % (self.b, self.k, self.x1, self.x2, self.accurate)

    def full_str(self):
        return "(%d, %.4f, %d, %d, %s) " % (self.b, self.k, self.x1, self.x2, self.accurate) + str(self._points)

    def is_valid(self, x):
        if not (self.x1 <= x and x <= self.x2):
            return False
        if self.consecutive:
            return (x - self.x1) % self.rec_k == 0
        else:
            if Segment.BITMAP:
                return self.filter[x - self.x1]
            else:
                return self.filter.check(x)

    def get_y(self, x, check=True):
        if not check or self.is_valid(x):
            predict = int(round(x*self.k + self.b))
            # lowbound = self.blocknum * Segment.PAGE_PER_BLOCK
            # upbound = (self.blocknum + 1) * Segment.PAGE_PER_BLOCK - 1
            # return max(min(predict, upbound), lowbound)
            return predict
        return None

    # @return is_accruate, is_consecutive
    def check_properties(self, points):
        is_accruate, is_consecutive = True, True
        for pt in points:
            if self.get_y(pt[0], check=False) != pt[1]:
                is_accruate = False
            # if abs(self.get_y(pt[0], check=False) - pt[1]) > 5:
            #     print(self, self.get_y(pt[0], check=False), pt[1])
        if len(np.unique(np.diff(zip(*points)[0]))) > 1:
            is_consecutive = False

        return is_accruate, is_consecutive

    def overlaps(self, other):
        return min(self.x2, other.x2) - max(self.x1, other.x1) >= 0

    def overlaps_with_range(self, x1, x2):
        return min(self.x2, x2) - max(self.x1, x1) >= 0


    # check whether two segments can be put into the same level
    # if they can be put in the same level, return False
    # (here we assume other is older than self)
    # def conflict(self, other):
    #     if self.overlaps(other) < 0:
    #         return False
    #     # (self.x1 - other.x1) % self.rec_k == 0 is necessary -- consider (698, 700, 702) followed by (701, 703, 705)
    #     if self.k == other.k and self.accurate and other.accurate and not (other.x1 < self.x1 and self.x2 < other.x2) and (self.x1 - other.x1) % self.rec_k == 0:
    #         return False
    #     else:
    #         return True

    # @return new, old, same_level
    @staticmethod
    def merge(new, old):
        if not new.overlaps(old):
            return new, old, True

        if not new.mergable or not old.mergable:
            return new, old, False

        if new.consecutive and old.consecutive:
            if new.rec_k == old.rec_k and (new.x1 - old.x1) % old.rec_k == 0:
                if new.x1 <= old.x1 and old.x2 <= new.x2:
                    return new, None, True
                elif old.x1 < new.x1 and new.x2 < old.x2:
                    return new, old, False
                elif new.x1 <= old.x1:
                    old.x1 = new.x2 + new.rec_k
                    return new, old, True
                elif old.x1 < new.x1:
                    old.x2 = new.x1 - new.rec_k
                    return new, old, True
            # TODO: optimize for two-point segments
            # else:
            #     return new, old, False

        new, old = Segment.bitwise_merge(new, old)
        if not old:
            return new, None, True
        if not new.overlaps(old):
            return new, old, True
        else:
            return new, old, False


    @staticmethod
    def bitwise_merge(new, old):
        lo, hi = min(old.x1, new.x1), max(old.x2, new.x2)
        new_bm = bitarray.bitarray(hi-lo+1)
        old_bm = bitarray.bitarray(hi-lo+1)
        new_bm.setall(0)
        old_bm.setall(0)

        if new.consecutive:
            new_bm[new.x1-lo : new.x2-lo+1 : new.rec_k] = 1
        elif Segment.BITMAP:
            new_bm[new.x1-lo : new.x2-lo+1] = new.filter
        
        if old.consecutive:
            old_bm[old.x1-lo : old.x2-lo+1 : old.rec_k] = 1
        elif Segment.BITMAP:
            old_bm[old.x1-lo : old.x2-lo+1] = old.filter
        
        try:
            old_bm = old_bm & (~new_bm)
        except:
            print(lo, hi)
            print(old, new)
            print(old._points, new._points)
            print(old_bm, new_bm)
            exit(0)

        first_valid = old_bm.find(1)
        if first_valid == -1:
            return new, None
        last_valid = bitarray.util.rindex(old_bm, 1)
        old.x1 = first_valid + lo 
        old.x2 = last_valid + lo

        if not old.consecutive and Segment.BITMAP:
            old.filter = old_bm[first_valid : last_valid+1]
            assert(old.filter != None)

        # TODO: re-check accuracy and consecutive

        return new, old

    @property
    def consecutive(self):
        return not self.filter

    @property
    def mergable(self):
        return self.consecutive or Segment.BITMAP

    @property
    def length(self):
        return (self.x2-self.x1) // self.rec_k + 1

    @property
    def memory(self):
        if self.x1 == self.x2:
            return SUBLPN_BYTES + PPN_BYTES # + FLOAT16_BYTES + LENGTH_BYTES
        else:
            if self.consecutive:
                return SUBLPN_BYTES + PPN_BYTES + FLOAT16_BYTES + LENGTH_BYTES # 4+4+2+1
            else:
                if Segment.BITMAP:
                    # filter_size = len(self.filter) / 8.0
                    ones = len([e for e in self.filter if e])
                    non_consec_ones = len([i for i in range(len(self.filter)) if i > 0 and i < len(self.filter)-1 and self.filter[i] is not self.filter[i-1]])
                    # print(self.filter, [i for i in range(len(self.filter)) if i > 0 and i < len(self.filter)-1 and self.filter[i] is not self.filter[i-1]])
                    # zeros = len(self.filter) - ones
                    # sparse_encoding_size = min(ones, zeros) * 1 + LENGTH_BYTES
                    return SUBLPN_BYTES + PPN_BYTES + FLOAT16_BYTES + non_consec_ones * 1 + LENGTH_BYTES 
                    #return SUBLPN_BYTES + PPN_BYTES + FLOAT16_BYTES + min(filter_size, sparse_encoding_size)
                else:
                    return SUBLPN_BYTES + PPN_BYTES + FLOAT16_BYTES + round(self.filter.bit_array_size / 8.0)
    
    @property
    def rec_k(self):
        return int(round(1.0/self.k))
    @property
    def blocknum(self):
        mid = (self.x2 + self.x1) / 2.0
        predict = int(round(mid * self.k + self.b))
        return int(predict / Segment.PAGE_PER_BLOCK)


class PLR():
    FIRST = "first"
    SECOND = "second"
    READY = "ready"

    def __init__(self, gamma):
        self.gamma = gamma
        self.max_length = 256
        self.init()

    def init(self):
        # temp states to build one next segment
        self.segments = []
        self.s0 = None
        self.s1 = None
        self.rho_upper = None
        self.rho_lower = None
        self.sint = None
        self.state = PLR.FIRST
        self.points = []

    # point (x, y)
    def learn(self, points):
        rejs = []
        count = 0
        size = len(points)
        for i, point in enumerate(points):
            seg, rej = self.process(point)
            if seg != None:
                self.segments.append(seg)
            if rej != None:
                rejs.append(rej)
    
        seg = self.build_segment()
        if seg != None:
            self.segments.append(seg)

        return self.segments

    def should_stop(self, point):
        if self.s1 is None:
            if point[0] > self.s0[0] + self.max_length:
                return True
        elif point[0] > self.s1[0] + self.max_length:
            return True
        # if len(self.points) >= 2 and (len(self.points) + 1) / float(point[0] - self.s0[0]) >= 0.9 and point[0] - self.points[-1][0] != self.points[-1][0] - self.points[-2][0]:
        #     return True
        return False

    def build_segment(self):
        if self.state == PLR.FIRST:
            seg = None
        elif self.state == PLR.SECOND:
            seg =  Segment(1, self.s0[1] - self.s0[0], self.s0[0], self.s0[0],
                           points=self.points)
        elif self.state == PLR.READY:
            avg_slope = np.float16((self.rho_lower.k + self.rho_upper.k) / 2.0)
            # avg_slope = (self.rho_lower.k + self.rho_upper.k) / 2.0
            # rec_k = round(1.0/avg_slope)
            intercept = -self.sint[0] * avg_slope + self.sint[1]
            seg = Segment(avg_slope, intercept, self.s0[0], self.s1[0],
                           points=self.points)
    
        return seg

    def process(self, point):
        prev_segment = None
        if self.state == PLR.FIRST:
            self.s0 = point
            self.state = PLR.SECOND

        elif self.state == PLR.SECOND:
            if self.should_stop(point):
                prev_segment = self.build_segment()
                self.s0 = point
                self.state = PLR.SECOND
                self.points = []

            else:
                self.s1 = point
                self.state = PLR.READY
                self.rho_lower = SimpleSegment.frompoints(SimpleSegment.get_upper_bound(self.s0, self.gamma),
                                                    SimpleSegment.get_lower_bound(self.s1, self.gamma))
                self.rho_upper = SimpleSegment.frompoints(SimpleSegment.get_lower_bound(self.s0, self.gamma),
                                                    SimpleSegment.get_upper_bound(self.s1, self.gamma))
                self.sint = SimpleSegment.intersection(
                    self.rho_upper, self.rho_lower)
                self.state = PLR.READY

        elif self.state == PLR.READY:
            if not SimpleSegment.is_above(point, self.rho_lower) or not SimpleSegment.is_below(point, self.rho_upper) or self.should_stop(point):
                prev_segment = self.build_segment()
                self.s0 = point
                self.state = PLR.SECOND
                self.points = []

            else:
                self.s1 = point
                s_upper = SimpleSegment.get_upper_bound(point, self.gamma)
                s_lower = SimpleSegment.get_lower_bound(point, self.gamma)
                if SimpleSegment.is_below(s_upper, self.rho_upper):
                    self.rho_upper = SimpleSegment.frompoints(self.sint, s_upper)
                if SimpleSegment.is_above(s_lower, self.rho_lower):
                    self.rho_lower = SimpleSegment.frompoints(self.sint, s_lower)

                # self.sint = SimpleSegment.intersection(
                #     self.rho_upper, self.rho_lower)


        self.points.append(point)

        return (prev_segment, None)


# bisect wrapper
class KeyWrapper:
    def __init__(self, iterable, key):
        self.it = iterable
        self.key = key

    def __getitem__(self, i):
        return self.key(self.it[i])

    def __len__(self):
        return len(self.it)


class LogPLR():
    def __init__(self, gamma, frame_no):
        self.plr = PLR(gamma)
        # one run is one level of segments with non-overlapping intervals
        self.runs = []
        self.frame_no = frame_no
        # mapping from block to segments
        # self.block_map = defaultdict(list)
    
    def update(self, entries, blocknum):
        # make sure no same LPNs exist in the entries
        sorted_entries = sorted(entries)
        # make sure no same 'x1's exist in the new_segments
        self.plr.init()
        new_segments = self.plr.learn(sorted_entries)
        new_segments = sorted(new_segments, key=lambda x: x.x1)
        # self.block_map[blocknum].extend(new_segments)
        # make sure no overlap at each level
        self.add_segments(0, new_segments, recursive=False)
        return [], []

    def merge(self, old_plr):
        assert(self.frame_no == old_plr.frame_no)
        self.runs.extend(old_plr.runs)

    # bottleneck
    def lookup(self, LPA, first=True):
        empty_levels = []
        results = []
        lookup = 0
        for level, run in enumerate(self.runs):
            if len(run) == 0:
                empty_levels.append(level)
                continue
            lookup += 1
            index = bisect_left(KeyWrapper(run, key=lambda seg: seg.x1), LPA)
            if index == 0 or (index < len(run) and run[index].x1 == LPA):
                seg = run[index]
            else:
                seg = run[index - 1]
            PPA = seg.get_y(LPA)
            if PPA:
                results += [(PPA, seg.accurate, seg)]
                if first:
                    break
            # if lookup > 5:
            #     print(self)
            #     self.compact()
            #     print(self)
            #     exit()

        for level in sorted(empty_levels, reverse=True):
            del self.runs[level]
        
        return results, lookup, [], []

    def lookup_range(self, start, end):
        results = defaultdict(list)
        for level, run in enumerate(self.runs):
            if len(run) == 0:
                continue
            index = bisect_left(KeyWrapper(run, key=lambda seg: seg.x1), start)
            if index == 0 or (index < len(run) and run[index].x1 == start):
                pass
            else:
                index = index - 1

            while index < len(run):
                seg = run[index]
                if seg.overlaps_with_range(start, end):
                    results[level].append(seg)
                    index += 1
                else:
                    break

        return results
            


    # recursively add segments to each level
    # bottleneck
    def add_segments(self, level, segments, recursive=True):
        while len(self.runs) <= level:
            self.runs.append([])

        run = self.runs[level]
        conflicts = []
        for new_seg in segments:
            if new_seg.get_y(LPN_TO_DEBUG):
                log_msg("%s added to run %d" % (new_seg, level))
            if len(run) == 0:
                run.append(new_seg)
                continue


            # run[index].x1 >= new_seg.x1
            index = bisect_left(KeyWrapper(
                run, key=lambda seg: seg.x1), new_seg.x1)
            run.insert(index, new_seg)
            overlaps = []
            if index != 0:
                overlaps.append((index-1, run[index-1]))
            for i in range(index+1, len(run)):
                if run[i].x1 > new_seg.x2:
                    break
                overlaps.append((i, run[i]))

            indices_to_delete = []
            for index, old_seg in overlaps:
                to_print = old_seg.get_y(LPN_TO_DEBUG)
                if to_print:
                    log_msg("%s tries to merge with %s" % (new_seg, old_seg))
                new_seg, old_seg, same_level = Segment.merge(new_seg, old_seg)

                if not old_seg:
                    indices_to_delete.append(index)
                    if to_print:
                        log_msg("%s removed old seg" % (new_seg))
                elif not same_level:
                    conflicts.append(old_seg)
                    indices_to_delete.append(index)
                    if to_print:
                        log_msg("%s -> %s" % (new_seg, old_seg))
            for index in sorted(indices_to_delete, reverse=True):
                if run[index].get_y(LPN_TO_DEBUG):
                    log_msg("removed old seg", (run[index]))
                del run[index]

        if recursive:
            if len(conflicts) > 0:
                self.add_segments(level+1, conflicts)
        else:
            if len(conflicts) > 0:
                self.runs.insert(level+1, conflicts)
            

    

    def __str__(self):
        repr = ""
        for level in range(len(self.runs)):
            repr += "== level %d ==\n %s \n" % (level, str(self.runs[level]))
        return repr

    @property
    def segments(self):
        return [seg for run in self.runs for seg in run]

    @property
    def memory(self):
        return sum([seg.memory for i, run in enumerate(self.runs) for seg in run]) + LPN_BYTES

    @property
    def levels(self):
        return len(self.runs)

    # now we only use compact
    def gc(self, blocknum):
        return 
        for seg in self.block_map[blocknum]:
            for run in reversed(self.runs):
                index = bisect_left(KeyWrapper(run, key=lambda _: _.x1), seg.x1)
                if index < len(run) and seg == run[index]:
                    if seg.get_y(LPN_TO_DEBUG):
                        log_msg("gc removed old seg", (seg))
                    run.remove(seg)
                    break
 
        self.block_map[blocknum] = []

    def promote(self):
        if len(self.runs) == 0:
            return

        if self.levels <= 10:
            return

        layers = self.runs[:]
        for i in range(1,len(layers)):
            lower_layer = layers[i]
            for old_seg in reversed(lower_layer):
                promoted_layer = i
                promoted_index = None
                for j in reversed(range(0,i)):
                    upper_layer = layers[j]
                    index = bisect_left(KeyWrapper(upper_layer, key=lambda seg: seg.x1), old_seg.x1)
                    overlaps = False
                    for k in range(max(0, index-1), len(upper_layer)):
                        if upper_layer[k].x1 > old_seg.x2:
                            break
                        if upper_layer[k].overlaps(old_seg):
                            overlaps = True
                            break
                    if overlaps:
                        break
                    else:
                        promoted_layer = j
                        promoted_index = index
                
                if promoted_layer < i:
                    # if self.frame_no == 3075:
                    #     log_msg("Promote %s to level %d" % (old_seg, promoted_layer))
                    layers[promoted_layer].insert(promoted_index, old_seg)
                    lower_layer.remove(old_seg)
        
        self.runs = [run for run in self.runs if len(run) != 0]
                  
    def compact(self, promote=False):
        if len(self.runs) == 0:
            return

        # relearn_dict = dict()
        layers = self.runs[:1]
        for layer in layers:
            for seg in layer:
                self.compact_range(seg.x1, seg.x2)
                # relearn_dict.update(relearn)

        self.runs = [run for run in self.runs if len(run) != 0]
        if promote:
            self.promote()

        return#len(self.segments), relearn_dict

    def compact_range(self, start, end):
        results = self.lookup_range(start, end)
        # relearn = dict()
        for upper_layer, new_segs in results.items():
            for lower_layer, old_segs in results.items():
                if upper_layer < lower_layer:
                    for new_seg in new_segs:
                        for old_seg in old_segs:
                            # if old_seg not in relearn and old_seg.x1 < new_seg.x1 and new_seg.x2 < old_seg.x2:
                            #     if not old_seg.consecutive:
                            #         relearn[old_seg] = sum(old_seg.filter) - 2
                            new_seg, updated_old_seg, same_level = Segment.merge(new_seg, old_seg)
                            if not updated_old_seg:
                                self.runs[lower_layer].remove(old_seg)
                                results[lower_layer].remove(old_seg)
        # return dict() #relearn


# Distribute the mapping entries into LPN ranges
# Each LogPLR is responsible for one range
class FrameLogPLR:
    ON_FLASH, CLEAN, DIRTY = "ON_FLASH", "CLEAN", "DIRTY"
    def __init__(self, conf, metadata, counter, gamma, max_size=1*1024**2, frame_length=256):
        global SUBLPN_BYTES
        SUBLPN_BYTES = 1
        self.conf = conf
        self.metadata = metadata
        self.counter = counter
        self.gamma = gamma
        self.frame_length = frame_length
        self.frames = LRUCache()
        self.max_size = self.conf['mapping_cache_bytes']
        # assert(self.max_size >= self.conf.page_size)

        # internal_type = "sftl"
        if self.conf['internal_ftl_type'] == "sftl":
            self.type = "sftl"
            self.frame_length = 1024
        elif self.conf['internal_ftl_type'] == "dftldes":
            self.type = "dftldes"
            self.frame_length = 1024
        else:
            self.type = "learnedftl"
            self.frame_length = 256

        self.GTD = dict()
        self.current_trans_block = None
        self.current_trans_page_offset = 0
        self.frame_on_flash = dict()
        self.memory_counter = dict()
        self.total_memory = 0
        self.hits = 0
        self.misses = 0
        self.dirty = dict()

    def create_frame(self, frame_no):
        if self.type == "learnedftl":
            return LogPLR(self.gamma, frame_no)
        
        elif self.type == "sftl":
            return SFTLPage(frame_no, self.frame_length)

        elif self.type == "dftldes":
            return DFTLPage(frame_no)
        
        else:
            raise NotImplementedError

    @staticmethod
    def split_into_frame(frame_length, entries):
        split_results = defaultdict(list)
        for lpn, ppn in entries:
            split_results[lpn // frame_length].append((lpn, ppn))
        return split_results

    def update(self, entries, blocknum):
        pages_to_write, pages_to_read = [], []
        split_entries = FrameLogPLR.split_into_frame(self.frame_length, entries)
        frame_nos = []
        for frame_no, entries in split_entries.items():
            frame_nos += [frame_no]
            if frame_no not in self.frames:
                self.frames[frame_no] = self.create_frame(frame_no)
                self.counter["mapping_table_write_miss"] += 1
            else:
                self.counter["mapping_table_write_hit"] += 1
            self.frames[frame_no].update(entries, blocknum)
            if self.frames[frame_no].levels > 0:
                # self.frames[frame_no].compact()
                self.frames[frame_no].promote()

            self.dirty[frame_no] = True

            self.change_size_of_frame(frame_no, self.frames[frame_no].memory)
        
        if self.should_flush():
            pages_to_write, pages_to_read = self.flush()

        return pages_to_write, pages_to_read

    def lookup(self, lpn, first=True):
        should_print = False
        frame_no = lpn // self.frame_length
        results = []
        pages_to_write, pages_to_read = [], []

        if frame_no in self.frames:
            # self.hits += 1
            frame = self.frames[frame_no]
            self.frames.move_to_head(frame_no, frame)
            # log_msg("Move to head", frame_no)
            results, lookup, _, _ = frame.lookup(lpn, first)
            
        if len(results) != 0:
            self.counter["mapping_table_read_hit"] += 1
        else:
            self.counter["mapping_table_read_miss"] += 1
            frame = self.frame_on_flash[frame_no]
            blocknum = self.GTD[frame_no]
            results, lookup, _, _ = frame.lookup(lpn, first)
            pages_to_read = [blocknum]

            if frame_no in self.frames:
                # log_msg(frame_no, "merged with memory")
                self.frames[frame_no].merge(frame)
                # self.frames[frame_no].compact()
                del self.frame_on_flash[frame_no]
            else:
                # log_msg(frame_no, "brought to memory")
                self.dirty[frame_no] = False
                self.frames[frame_no] = frame
                del self.frame_on_flash[frame_no]
                
            self.frames.move_to_head(frame_no, self.frames[frame_no])
            # log_msg("Move to head", frame_no)
            # log_msg(self.frames.keys())
            self.change_size_of_frame(frame_no, self.frames[frame_no].memory)

        if self.memory > self.max_size:
            should_print = True

        # if should_print:
        #     print("before",self.memory)

        if self.should_flush():
            mapping_pages_to_write, mapping_pages_to_read = self.flush()
            pages_to_read += mapping_pages_to_read
            pages_to_write += mapping_pages_to_write

        # if should_print:
        #     print("after",self.memory)
        
        return results, lookup, pages_to_write, pages_to_read

    # gc is currently replaced with compaction
    def gc(self, blocknum):
        pass

    def compact(self, promote=False, frame_nos=None):
        # relearn_dict = dict()
        # total_segments = 0
        if not frame_nos:
            for frame_no, frame in self.frames.items():
                frame.compact(promote=promote)
                # total_segments += total_segs
                # relearn_dict.update(relearn)
                self.change_size_of_frame(frame_no, frame.memory) 
        else:
            for frame_no in frame_nos:
                frame = self.frames[frame_no]
                frame.compact(promote=promote)
                self.change_size_of_frame(frame_no, frame.memory) 
        
        # appox_segments = 0
        # appox_segments_length = []
        # for seg in self.segments:
        #     if not seg.consecutive:
        #         appox_segments += 1
        #         appox_segments_length.append(sum(seg.filter) - 2)
        # print(len(self.segments), appox_segments, np.average(appox_segments_length))
        # print(total_segments, len(relearn_dict), sum(relearn_dict.values()))

    def promote(self):
        for frame in self.frames.values():
            frame.promote()


    def should_flush(self):
        if self.memory > self.max_size:
            # n_pages = (self.memory - self.max_size) /self.conf.page_size
            return True
        else:
            return False

    def allocate_ppn_for_frame(self, frame_no):
        if not self.current_trans_block or self.current_trans_page_offset == 256:
            self.current_trans_block = self.metadata.bvc.next_free_block()
            self.metadata.pvb.validate_block(self.current_trans_block)

        next_free_ppn = self.conf.n_pages_per_block * self.current_trans_block + self.current_trans_page_offset
        self.current_trans_page_offset += 1

        if frame_no not in self.GTD:
            old_ppn = None
            self.GTD[frame_no] = next_free_ppn
            new_ppn = self.GTD[frame_no]
            
        else:
            old_ppn = self.GTD[frame_no]
            self.GTD[frame_no] = next_free_ppn
            new_ppn = self.GTD[frame_no]

        return new_ppn, old_ppn
        
    def flush(self):
        evicted_frames = []
        pages_to_read = []
        pages_to_write = []


        original_memory = self.memory
        # assert(self.memory == sum([frame.memory for frame in self.frames.values()]))
        while original_memory > self.max_size:
            frame_no, evict_frame = self.frames.popitem(last=False)
            # log_msg(frame_no, "evicted")
            freed_mem = self.memory_counter[frame_no]
            original_memory -= freed_mem
            self.change_size_of_frame(frame_no, 0) 
            evicted_frames.append(frame_no)

            new_ppn, old_ppn = self.allocate_ppn_for_frame(frame_no)
            if self.dirty[frame_no]:
                self.counter["flush mapping table"] += 1
            self.dirty[frame_no] = False
            pages_to_write.append(new_ppn)

            if frame_no in self.frame_on_flash:
                # log_msg(frame_no, "merged with flash")
                old_frame = self.frame_on_flash[frame_no]
                pages_to_read += [old_ppn]
                evict_frame.merge(old_frame)
            self.frame_on_flash[frame_no] = evict_frame


        # log_msg("%.2f miss ratio, %s evicted, %d memory, %d in cache, %d on flash" % (self.misses / float(self.misses + self.hits), evicted_frames, self.memory, len(self.frames), len(self.frame_on_flash)))
        # log_msg("%d miss, %d memory flushed, %s evicted, %d memory, %d in cache, %d on flash" % (self.misses, original_memory - self.memory, evicted_frames, self.memory, len(self.frames), len(self.frame_on_flash)))

        return pages_to_write, list(set(pages_to_read))

    def change_size_of_frame(self, frame_no, new_mem):
        old_mem = 0
        if frame_no in self.memory_counter:
            old_mem = self.memory_counter[frame_no]
        self.memory_counter[frame_no] = new_mem
        self.total_memory += (new_mem - old_mem)
  
    # TODO: bottleneck
    @property
    def memory(self):
        # assert(self.total_memory == sum([mem for frame_no, mem in self.memory_counter.items()]))
        return self.total_memory
        # return sum([mem for frame_no, mem in self.memory_counter.items()])

    @property
    def segments(self):
        return [seg for frame in self.frames.values() for seg in frame.segments]

    @property
    def levels(self):
        if len(self.frames) == 0:
            return 0
        return max([frame.levels for frame in self.frames.values()])

    @property
    def dist_levels(self):
        if len(self.frames) == 0:
            return 0, 0
        dist = [frame.levels for frame in self.frames.values() if frame.levels != 0]
        return dist

        
class PFTL(object):
    def __init__(self):
        # store update history for verification purpose
        self.mapping_table = defaultdict(list)

    def set(self, lpn, ppn):
        self.mapping_table[lpn].append(ppn)
            
    def get(self, lpn):
        # force check; since we are using defaultdict we don't want to create empty entry
        if lpn not in self.mapping_table:
            return None
        ppns = self.mapping_table[lpn]
        if len(ppns) > 0:
            return ppns[-1]

    def get_all(self, lpn):
        if lpn not in self.mapping_table:
            return None
        return self.mapping_table[lpn]

    @property
    def memory(self):
        return len(self.mapping_table) * (PPN_BYTES + LPN_BYTES)



def split_ext(extent):
    if extent.lpn_count == 0:
        return None

    exts = []
    for lpn in extent.lpn_iter():
        cur_ext = Extent(lpn_start=lpn, lpn_count=1)
        exts.append(cur_ext)

    return exts

# no need to dump the entire timeline if storage space is limited
def write_timeline(conf, recorder, op_id, op, arg, start_time, end_time):
    return
    recorder.write_file('timeline.txt',
            op_id = op_id, op = op, arg = arg,
            start_time = start_time, end_time = end_time)