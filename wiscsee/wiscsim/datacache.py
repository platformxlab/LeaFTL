#!/usr/bin/env python2
from collections import OrderedDict, defaultdict
import numpy as np
from wiscsim.utils import *

class LFUCache(OrderedDict):
    def __init__(self):
        self.freq = defaultdict(list)
        super(LFUCache, self).__init__()

    def append(self, key):
        self[key] = ""
    
    def evict(self, n):
        for _ in range(n):
            for l in self.freq.values():
                if len(l) == 0:
                    continue
                del self[l[0]]
                del l[0]
                break
            #self.popitem(last=True)

    def _write(self, lpn):
        self[lpn] = 0
        self.freq[0].append(lpn)

    def _read(self, lpn):
        self.freq[self[lpn]].remove(lpn)
        self[lpn] = self[lpn] + 1
        self.freq[self[lpn]].append(lpn)
        # self = sorted(self,key=self.get)
        # self.move_to_head(lpn, self[lpn])

    # def move_to_head(self, key, value, dict_setitem=dict.__setitem__):
    #     del self[key]
    #     self[key] = value


class LIFOCache(OrderedDict):
    def __init__(self):
        super(LIFOCache, self).__init__()

    def append(self, key):
        self[key] = ""

    def evict(self, n):
        for _ in range(n):
            self.popitem(last=True)

    def _write(self, lpn):
        self[lpn] = False
    def _read(self, lpn):
        pass
        # self.move_to_head(lpn, self[lpn])

    def move_to_head(self, key, value, dict_setitem=dict.__setitem__):
        del self[key]
        self[key] = value


class FIFOCache(OrderedDict):
    def __init__(self):
        super(FIFOCache, self).__init__()

    def append(self, key):
        self[key] = ""

    
    def evict(self, n):
        for _ in range(n):
            self.popitem(last=False)

    def _write(self, lpn):
        self[lpn] = False
    def _read(self, lpn):
        pass
        # self.move_to_head(lpn, self[lpn])

    def move_to_head(self, key, value, dict_setitem=dict.__setitem__):
        del self[key]
        self[key] = value

class MRUCache(OrderedDict):
    def __init__(self):
        super(MRUCache, self).__init__()

    def append(self, key):
        self[key] = ""
    
    def evict(self, n):
        for _ in range(n):
            self.popitem(last=True)

    def _write(self, lpn):
        self[lpn] = False
    def _read(self, lpn):
        self.move_to_head(lpn, self[lpn])

    def move_to_head(self, key, value, dict_setitem=dict.__setitem__):
        del self[key]
        self[key] = value

class LRUCache(OrderedDict):
    def __init__(self):
        super(LRUCache, self).__init__()

    def append(self, key):
        self[key] = ""

    # FIXME: bug popitem
    def evict(self, n):
        evicted = []
        for _ in range(n):
            if len(self) == 0:
                return None
            lpn, state = self.popitem(last=False)
            evicted.append((lpn, state))
        return evicted

    def evict_one(self):
        evicted = self.evict(1)
        return evicted[0]

    def evict_lpn(self, lpn):
        state = self[lpn]
        del self[lpn]
        return (lpn, state)

    def _write(self, lpn, state=""):
        self[lpn] = state
    
    def _read(self, lpn):
        self.move_to_head(lpn, self[lpn])
        return self[lpn]

    def peak(self):
        return next(self.iteritems())

    def peak_n(self, n):
        return dict([next(self.iteritems()) for i in range(n)])

    def move_to_head(self, key, value, dict_setitem=dict.__setitem__):
        del self[key]
        self[key] = value
        # print(dir(self))
        # root = self._OrderedDict__root
        # first = root[1]

        # if key in self:
        #     if first[2] != key:
        #         link = self._OrderedDict__map[key]
        #         link_prev, link_next, _ = link
        #         link_prev[1] = link_next
        #         link_next[0] = link_prev
        #         link[0] = root
        #         link[1] = first
        #         root[1] = first[0] = link
        # else:
        #     root[1] = first[0] = self._OrderedDict__map[key] = [root, first, key]
        # dict_setitem(self, key, value)


"""
    self.cache = {LPN : priority in {True or False}}
    self.high = # of entries with priority
"""
class DataCache(object):
    def __init__(self, size, slot_size, method="LRU", priority=False, threshold=0.2):
        
        assert(method in ["LRU","FIFO","LFU","LIFO","MRU"])
        method2cache = {"LRU": LRUCache(), 
            "FIFO": FIFOCache(),
            "LFU": LFUCache(),
            "LIFO": LIFOCache(),
            "MRU": MRUCache()
        }
        self.cache = method2cache[method]
        self.slot_size = slot_size
        self.capacity = max(1, self.bytes2slots(size))
        self.priority = priority
        self.high = 0.
        self.threshold = threshold

    def bytes2slots(self, size):
        return max(int(size / self.slot_size), 0)
        
    def read(self, lpn):
        if lpn not in self.cache:
            self._write(lpn)
            return False
        else:
            self.cache._read(lpn)
            # self.cache.move_to_head(lpn, self.cache[lpn])
            return True

    def invalidate(self, lpn):
        if lpn in self.cache:
            # priority = self.cache[lpn]
            del self.cache[lpn]
            # if priority:
            #     self.high -= 1

    def set_priority(self, lpn, priority):
        old_priority = self.cache[lpn]
        if lpn in self.cache:
            self.cache[lpn] = priority
            if not old_priority and priority:
                self.high += 1
            if old_priority and not priority:
                self.high -= 1


    def _write(self, lpn):
        # self.cache[lpn] = False
        # self.cache.move_to_head(lpn, self.cache[lpn])
        if len(self.cache) >= self.capacity:
            self.evict(1)
        self.cache._write(lpn)

    def evict(self, n):
        assert(n <= self.capacity)
        self.cache.evict(n)
        # for _ in range(n):
        #     # guarantee to evict low priority
        #     # if self.priority and self.high <= self.threshold * self.capacity:
        #     #     for lpn, priority in reversed(self.cache.items()):
        #     #         if not priority:
        #     #             del self.cache[lpn]
        #     #             break
        #     # else:
        #     lpn, priority = self.cache.popitem(last=False)
                # if priority:
                #     self.high -= 1

    def resize(self, size):
        assert(size >= 0)
        old_capacity = self.capacity
        self.capacity = self.bytes2slots(size)
        if self.capacity < old_capacity:
            self.evict(old_capacity - self.capacity)


class WriteBuffer():
    UNASSIGNED, ASSIGNED = "UNASSIGNED", "ASSIGNED"
    def __init__(self, flush_size, size, filtering = 1.0, assign_ppn=True):
        assert(flush_size <= size)
        self.assign_ppn = assign_ppn
        self.flush_size = flush_size
        self.size = size
        self.assigned = LRUCache()
        self.unassigned = LRUCache()
        self.mapping = dict()
        self.counter = 0
        self.filtering = filtering

    def peek(self, lpn, move=False):
        if lpn in self.assigned:
            if move:
                self.assigned.move_to_head(lpn, self.assigned[lpn])
            return WriteBuffer.ASSIGNED
        elif lpn in self.unassigned:
            if move:
                self.unassigned.move_to_head(lpn, self.unassigned[lpn])
            return WriteBuffer.UNASSIGNED 
        else:
            return None 

    def read(self, lpn):
        return self.peek(lpn, move=True)


    def write(self, lpn):
        if not self.peek(lpn):
            self.unassigned[lpn] = ""
        else:
            self.counter += 1
            #print("hit", self.counter)

        if self.length > self.size:
            writeback, _ = self.assigned.popitem(last=False)
            if self.assign_ppn:
                ppn = self.mapping[writeback]
                del self.mapping[writeback]
                return writeback, ppn
            else:
                return writeback, None

        else:
            return (None, None)
        

    def should_assign_page(self):
        return len(self.unassigned) >= self.flush_size

    @staticmethod
    def split_into_frame(frame_length, entries):
        split_results = defaultdict(list)
        for lpn, ppn in entries.items():
            split_results[lpn // frame_length].append((lpn, ppn))
        return split_results

    def flush_unassigned(self):
        assert(len(self.unassigned) == self.flush_size)
        split_results = self.split_into_frame(256, self.unassigned)
        per_frame_count = dict(sorted([(k, len(v)) for k,v in split_results.items()], key=lambda x:x[1], reverse=True))
        cumsum = np.cumsum(per_frame_count.values())
        index = np.argwhere(cumsum<=self.flush_size*self.filtering)[-1][0]
        lpa_to_flush = sorted(filter(lambda x: x // 256 in per_frame_count.keys()[:index+1], self.unassigned))

        for lpa in lpa_to_flush:
            self.assigned[lpa] = ""

        for lpn in lpa_to_flush:
            del self.unassigned[lpn]

        # buffer = sorted(self.unassigned)
        # self.assigned.update(self.unassigned)
        # self.unassigned = LRUCache()
        return lpa_to_flush

    def update_assigned(self, mappings):
        assert(self.assign_ppn)
        for lpn, ppn in mappings.items():
            assert(lpn in self.assigned)
            self.mapping[lpn] = ppn

    @property
    def length(self):
        return len(self.assigned) + len(self.unassigned)



class SimpleWriteBuffer():
    def __init__(self, size):
        # assert(size <= max_size)
        # self.max_size = max(size, max_size)
        self.size = size
        self.buffer = []
        self.counter = 0

    def read(self, lpn):
        return lpn in self.buffer

    def write(self, lpn):
        if lpn not in self.buffer:
            self.buffer.append(lpn)
        else:
            self.counter += 1
            #print("hit", self.counter)

    def should_flush(self):
        return self.length >= self.size

    def flush(self):
        written_buffer = self.buffer
        self.buffer = []
        return sorted(written_buffer)

    @property
    def length(self):
        return len(self.buffer)




class RWCache(object):
    def __init__(self, size, slot_size, flush_size, filter_ratio, preconditioning=0.2, method="LRU"):
        
        assert(method in ["LRU","FIFO","LFU","LIFO","MRU"])
        self.slot_size = slot_size
        if flush_size > size:
            flush_size = size
            filter_ratio = 1.0
            log_msg("Adjusting flush size to become the data cache size")
        self.capacity = max(int(size / self.slot_size), 1)
        assert(flush_size <= size)
        self.flush_size = max(int(flush_size / self.slot_size), 1) 

        self.cache = self.method2cache(method)
        self.clean = self.method2cache(method)
        self.assigned = self.method2cache(method)
        self.unassigned = self.method2cache(method)
        self.dirty_page_mapping = dict()
        self.filter_ratio = filter_ratio

        self.counter = defaultdict(float)

        for i in range(self.capacity):
            dummy_lpn = "dummy"+str(i)
            if i <= int(preconditioning*self.capacity):
                self.cache._write(dummy_lpn, state="A")
                self.assigned._write(dummy_lpn)
            else:
                self.cache._write(dummy_lpn, state="C")
                self.clean._write(dummy_lpn)
            # TODO: change 0 to a random ppn in the address space and keep them striped across different channels.
            self.dirty_page_mapping[dummy_lpn] = 0

    def method2cache(self, method):
        if method == "LRU":
            return LRUCache()
        elif method == "FIFO":
            return FIFOCache()
        elif method == "LFU":
            return LFUCache()
        elif method == "LIFO":
            return LIFOCache()
        elif method == "MRU":
            return MRUCache()
        
    def read(self, lpn):
        writeback, wb_ppn = False, None
        if lpn not in self.cache:
            self.counter["read misses"] += 1
            self.cache._write(lpn, state="C")
            self.clean._write(lpn)
            writeback, wb_ppn = self.evict(state_preference="C")
            hit = False
        else:
            self.counter["read hits"] += 1
            state = self.cache._read(lpn)
            if state == "U":
                self.unassigned._read(lpn)
            # if state == "A":
            #     self.assigned._read(lpn)
            if state == "C":
                self.clean._read(lpn)
            hit = True

        if writeback:
            self.counter["writebacks"] += 1
        
        return hit, writeback, wb_ppn

    def write(self, lpn):
        writeback, wb_ppn = False, None
        if lpn not in self.cache:
            self.counter["write misses"] += 1
            self.cache._write(lpn, state="U")
            self.unassigned._write(lpn)
            writeback, wb_ppn = self.evict(state_preference="C")

        else:
            self.counter["write hits"] += 1
            state = self.cache._read(lpn)
            if state == "U":
                self.unassigned._read(lpn)
            # if state == "A":
            #     self.assigned._read(lpn)
            if state == "C":
                self.clean.evict_lpn(lpn)
                self.unassigned._write(lpn)
                self.cache[lpn] = "U"

        if writeback:
            self.counter["writebacks"] += 1
        
        return writeback, wb_ppn
    
    def evict(self, state_preference):
        writeback, wb_ppn = False, None
        if self.should_evict():
            # if filter_state == "A":
            #     lpn, _ = self.assigned.evict_one()
            #     _, state = self.cache.evict_lpn(lpn)
            #     assert(state == filter_state)
            #     writeback, wb_ppn = True, self.dirty_page_mapping[lpn]
            #     del self.dirty_page_mapping[lpn]
            # elif filter_state == "C":
            #     lpn, _ = self.clean.evict_one()
            #     _, state = self.cache.evict_lpn(lpn)
            #     assert(state == filter_state)

            lpn, state = self.cache.peak()
            if state == "A":
                self.cache.evict_one()
                lpn, state = self.assigned.evict_lpn(lpn)
                writeback, wb_ppn = True, self.dirty_page_mapping[lpn]
                del self.dirty_page_mapping[lpn]
            elif state == "C":
                self.cache.evict_one()
                lpn, state = self.clean.evict_lpn(lpn)
            
            ## we still can hit U here if the size of unassigned is smaller than flush_size
            elif state == "U":
                if state_preference == "A" and len(self.assigned) == 0:
                    state_preference = "C"
                elif state_preference == "C" and len(self.clean) == 0:
                    state_preference = "A"
                                
                if state_preference == "A":
                    lpn, _ = self.assigned.evict_one()
                    _, state = self.cache.evict_lpn(lpn)
                    assert(state == state_preference)
                    writeback, wb_ppn = True, self.dirty_page_mapping[lpn]
                    del self.dirty_page_mapping[lpn]
                elif state_preference == "C":
                    lpn, _ = self.clean.evict_one()
                    _, state = self.cache.evict_lpn(lpn)
                    assert(state == state_preference)
        
        return writeback, wb_ppn

    
    def update_assigned(self, mappings):
        for lpn, ppn in mappings.items():
            assert(lpn in self.assigned)
            self.dirty_page_mapping[lpn] = ppn

    def select_unassigned(self):
        frame_length = 256
        split_results = defaultdict(list)
        candidates = []
        for i, (lpn, _) in enumerate(self.unassigned.items()):
            split_results[lpn // frame_length].append((lpn, _))
            candidates.append(lpn)
            if i >= self.flush_size:
                break
        per_frame_count = dict(sorted([(k, len(v)) for k,v in split_results.items()], key=lambda x:x[1], reverse=True))
        cumsum = np.cumsum(per_frame_count.values())

        ## select index as the last logical group to satisfy the flush size requirement
        index = np.argwhere(cumsum<=self.flush_size*self.filter_ratio)[-1][0]
        lpa_to_flush = sorted(filter(lambda x: x // frame_length in per_frame_count.keys()[:index+1], candidates))
        # lpa_to_flush = list(filter(lambda x: x // frame_length in per_frame_count.keys()[:index+1], candidates))

        return lpa_to_flush


    def flush_unassigned(self):
        # evicted = self.unassigned.evict(self.flush_size)
        # lpn_to_flush = zip(*evicted)[0]
        # candidates =  self.unassigned # self.unassigned.peak_n(self.flush_size)
        lpn_to_flush = self.select_unassigned()
        for lpn in lpn_to_flush:
            self.assigned[lpn] = ""
            del self.unassigned[lpn]
            self.cache[lpn] = "A"
                
        return lpn_to_flush

    def should_assign_page(self):
        return self.cache.peak()[1] == "U" and len(self.cache) >= self.capacity and len(self.unassigned) >= self.flush_size

    def should_evict(self):
        return len(self.cache) > self.capacity

    def record_counter(self):
        for k, v in self.counter.items():
            log_msg(k, v)
        
        num_writes = self.counter["write hits"] + self.counter["write misses"]
        num_reads = self.counter["read hits"] + self.counter["read misses"]
        if num_writes > 0:
            log_msg("write miss ratio", self.counter["writebacks"] / num_writes)
        if num_reads > 0:
            log_msg("read miss ratio", self.counter["read misses"] / num_reads)
        
        log_msg("Clean slots", len(self.clean), "Dirty slots (assigned)", len(self.assigned), "Dirty slots (unassigned)", len(self.unassigned))

        est_overall_latency = (self.counter["read misses"]*20 + self.counter["writebacks"]*200) / (num_writes + num_reads)
        if num_reads > 0:
            est_read_latency = self.counter["read misses"]*20 / num_reads
            log_msg("Estimated read latency", round(est_read_latency,3), "us")
        if num_writes > 0:
            est_write_latency = self.counter["writebacks"]*200 / num_writes
            log_msg("Estimated write latency", round(est_write_latency,3), "us")
        log_msg("Estimated overall latency", round(est_overall_latency,3), "us")
