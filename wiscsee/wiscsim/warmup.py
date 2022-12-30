from itertools import combinations
import bitarray
import bitarray.util
import numpy as np
from wiscsim.lsm_tree.bloom_filter import BloomFilter
from collections import defaultdict
from bisect import bisect_left, insort_left
from datacache import LRUCache
from wiscsim.utils import *
from wiscsim.workload_parser import parse_events, create_config, mix_events
from ftlsim_commons import Extent
from datacache import WriteBuffer, RWCache
from workflow import *
from wiscsim.sftl import SFTLPage
import glob
from joblib import Parallel, delayed

# some constants
LPN_BYTES = 4
SUBLPN_BYTES = 4  # = 1 if use FramePLR
PPN_BYTES = 4
FLOAT16_BYTES = 2
LENGTH_BYTES = 1  # MAX_SEGMENT_LENGTH = 256
LPN_TO_DEBUG = -1

KB = 1024.0
MB = 1024.0**2
GB = 1024.0**3
TB = 1024.0**4



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
        return len(self.mapping_table)*4

    @property
    def pages(self):
        return len(self.mapping_table)



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
        
        # if LPN_TO_DEBUG in zip(*points)[0]:
        #     log_msg("new seg", self)

        # if self.rec_k < 1:
        #     print(self.full_str())

    def __str__(self):
        return "%.4f, %d, [%d, %d], memory: %dB, accuracy: %s, bitmap: %s" \
            % (self.k, self.b, self.x1, self.x2, self.memory, self.accurate, self.filter)

    def __repr__(self):
        return str(self)
        return "(%d, %.4f, %d, %d, %s)" % (self.b, self.k, self.x1, self.x2, self.accurate)

    def short(self):
        return "%d,%d,%.3f,%d" % (self.x1, self.x2 - self.x1, self.k, self.b)

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
        if point[0] > self.s0[0] + self.max_length:
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
            # avg_slope = np.float16((self.rho_lower.k + self.rho_upper.k) / 2.0)
            avg_slope = (self.rho_lower.k + self.rho_upper.k) / 2.0
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
                # log_msg("%s tries to merge with %s" % (new_seg, old_seg))
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

        if self.levels <= 15:
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

        layers = self.runs[:1]
        for layer in layers:
            for seg in layer:
                self.compact_range(seg.x1, seg.x2)

        self.runs = [run for run in self.runs if len(run) != 0]
        if promote:
            self.promote()

    def compact_range(self, start, end):
        results = self.lookup_range(start, end)
        for upper_layer, new_segs in results.items():
            for lower_layer, old_segs in results.items():
                if upper_layer < lower_layer:
                    for new_seg in new_segs:
                        for old_seg in old_segs:
                            new_seg, updated_old_seg, same_level = Segment.merge(new_seg, old_seg)
                            if not updated_old_seg:
                                self.runs[lower_layer].remove(old_seg)
                                results[lower_layer].remove(old_seg)


# Distribute the mapping entries into LPN ranges
# Each LogPLR is responsible for one range
class FrameLogPLR:
    ON_FLASH, CLEAN, DIRTY = "ON_FLASH", "CLEAN", "DIRTY"
    def __init__(self, counter, gamma, max_size=1*1024**2, frame_length=256, ftl_type="learnedftl"):
        global SUBLPN_BYTES
        SUBLPN_BYTES = 1
        self.counter = counter
        self.gamma = gamma
        self.frame_length = frame_length
        self.frames = LRUCache()
        self.max_size = 1024**3
        self.n_pages_per_block = 256
        # assert(self.max_size >= self.conf.page_size)

        # self.type = "learnedftl"
        # self.frame_length = 256
        if ftl_type == "learnedftl":
            self.type = "learnedftl"
            self.frame_length = 256
        else:
            self.type = "sftl"
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
            results, lookup = frame.lookup(lpn, first)
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
        if not frame_nos:
            for frame_no, frame in self.frames.items():
                frame.compact(promote=promote)
                self.change_size_of_frame(frame_no, frame.memory) 
        else:
            for frame_no in frame_nos:
                frame = self.frames[frame_no]
                frame.compact(promote=promote)
                self.change_size_of_frame(frame_no, frame.memory) 

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
        # if not self.current_trans_block or self.current_trans_page_offset == 256:
        #     self.current_trans_block = self.metadata.bvc.next_free_block()
        #     self.metadata.pvb.validate_block(self.current_trans_block)

        next_free_ppn = self.n_pages_per_block * self.current_trans_block + self.current_trans_page_offset
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
        return [seg for frame in self.frames.values() for seg in reversed(frame.segments)]

    @property
    def levels(self):
        if len(self.frames) == 0:
            return 0
        return max([frame.levels for frame in self.frames.values()])

    @property
    def avg_levels(self):
        if len(self.frames) == 0:
            return 0, 0
        dist = [frame.levels for frame in self.frames.values() if frame.levels != 0]
        return np.average(dist), np.std(dist)
    
    @property
    def groups(self):
        return len(self.frames)

    def dump(self, filename):
        with open(filename, "w") as outfile:
            all_segments = [(frame_no, seg) for frame_no, frame in self.frames.items() for seg in reversed(frame.segments)]
            for frame_no, segment in all_segments:
                outfile.write(str(frame_no)+","+segment.short()+"\n")

def split_ext(extent):
    if extent.lpn_count == 0:
        return None

    exts = []
    for lpn in extent.lpn_iter():
        cur_ext = Extent(lpn_start=lpn, lpn_count=1)
        exts.append(cur_ext)

    return exts

if __name__ == "__main__":
    log_msg("warm-up starts")
    # traces = ["/home/js39/datasets/MSR-Cambridge/usr_0.csv"]
    #trace = '/home/js39/datasets/FIU/homes/homes-110108-112108.1.blkparse'
    #trace = "/home/js39/datasets/FIU/mail/cheetah.cs.fiu.edu-110108-113008.1.blkparse"
    #trace = "/home/js39/datasets/rocksdb/ssdtrace-00"
    # traces = ["MSR-Cambridge/usr_0.csv", "MSR-Cambridge/usr_1.csv", "MSR-Cambridge/usr_2.csv", "MSR-Cambridge/prn_0.csv", "MSR-Cambridge/src2_2.csv", "MSR-Cambridge/hm_0.csv", "MSR-Cambridge/prxy_0.csv", 'FIU/homes/homes-110108-112108.1.blkparse', 'FIU/mail/cheetah.cs.fiu.edu-110108-113008.1.blkparse']
    # traces = ["/home/js39/datasets/"+_ for _ in traces]
    # traces = glob.glob("/home/js39/datasets/"+"MSR-Cambridge/*.csv")
    # traces = glob.glob("/home/js39/datasets/*.trace")
    # traces += glob.glob("/home/js39/datasets/"+"FIU/homes/*.blkparse")
    # traces += glob.glob("/home/js39/datasets/"+"FIU/webmail/*.blkparse")
    # traces += glob.glob("/home/js39/datasets/"+"Financial/*")
    # traces = ["/home/js39/datasets/benchbase/seats_5.trace", "/home/js39/datasets/benchbase/tpcc_2.trace", "/home/js39/datasets/benchbase/auctionmark_1.trace"]
    # traces = ["/home/js39/datasets/benchbase/seats_5.trace"]
    # traces = ["/home/js39/datasets/benchbase/seats_large.trace"] # 40% 5%
    # traces = ["/home/js39/datasets/benchbase/auctionmark_large.trace"] # 10% 10%
    # traces = ["/home/js39/datasets/benchbase/oltp_3.trace"] # 0% 15%
    traces = ["/home/js39/datasets/benchbase/tpcc_2.trace"]

    page_size = 16384.0
    # page_size = 8192.0
    # page_size = 4096.0

    all_events = dict()
    for trace in traces:
        events = parse_events(trace, page_size=page_size, max_writes=2000000, recorder=False)
        all_events[trace] = events
    writes = mix_events(all_events, page_size, policy="RR_Single_Light_Shuffle")
    log_msg("Total # of writes:", len(writes))

    def warm_up(ftl_type, gamma):
        os.system("taskset -p 0xff %d > /dev/null" % os.getpid())
        if ftl_type == "learnedftl":
            filter_ratio = 7.0/8.0
        elif ftl_type == "sftl":
            filter_ratio = 1.0
        buffer = RWCache(8*MB, page_size, 8*MB, filter_ratio) # WriteBuffer(8*256, 8*256, filtering=1.0)
        counter =  defaultdict(float)
        mapping_table = FrameLogPLR(counter, gamma=gamma, ftl_type=ftl_type)
        reference_mapping_table = PFTL()
        conf = create_config(ftl_type="learnedftl")

        next_free_ppn = 0

        for op_i, write in enumerate(writes):
            if op_i % 100000 == 0 and op_i != 0:
                mapping_table.compact()
                mapping_table.promote()
                mapping_table.compact()
                log_msg((ftl_type, gamma), "Processed # of writes:", op_i, "Memory:", mapping_table.memory, reference_mapping_table.memory, mapping_table.groups)
                # mapping_table.compact()
                # mapping_table.promote()

            writeback, ppn = buffer.write(write.lpn_start)
            if buffer.should_assign_page():
                entries = []
                exts = buffer.flush_unassigned()
                for i, lpn in enumerate(exts):
                    next_free_ppn += 1
                    entry = (lpn, next_free_ppn)
                    reference_mapping_table.set(entry[0], entry[1])
                    entries.append(entry)
                mapping_pages_to_write, mapping_pages_to_read = mapping_table.update(entries, -1)
                buffer.update_assigned(dict(entries))

        mapping_table.compact()
        mapping_table.promote()
        mapping_table.compact()

        mapping_table.dump("/home/js39/software/wiscsee/wiscsim/compaction/segments.txt")

        log_msg("finish")

        return (mapping_table.memory, reference_mapping_table.memory)

    params = [('learnedftl', 1e-4), ("sftl", 0), ('learnedftl', 16)]
    # params = [("learnedftl", 1e-4)]

    results = Parallel(n_jobs=len(params), backend="multiprocessing")(delayed(warm_up)(ftl_type, gamma) for (ftl_type, gamma) in params)

    print(results)

    for i, (ftl_type, gamma) in enumerate(params):
        mapping_table_memory = results[i][0]
        reference_mapping_table_memory = results[i][1]

        log_msg(ftl_type, gamma)

        log_msg("estimated %s memory footprint: %d B" % (ftl_type, mapping_table_memory))
        log_msg("estimated dftl memory footprint: %d B" % reference_mapping_table_memory)
        log_msg("estimated storage consumption: %.2f GB" % float(reference_mapping_table_memory * page_size / 4.0 / GB))

        continue
        log_msg("== Scaled ==")
        storage = float(reference_mapping_table_memory / 4.0 * page_size)
        factor = 2*TB/storage

        log_msg("estimated %s memory footprint for 2TB: %d MB" % (ftl_type, mapping_table_memory * factor / MB))
        log_msg("estimated dftl memory footprint for 2TB: %d MB" % (reference_mapping_table_memory * factor / MB))
        log_msg("")

        storage = float(reference_mapping_table_memory / 4.0 * page_size)
        factor = 1*TB/storage

        log_msg("estimated %s memory footprint for 1TB: %d MB" % (ftl_type, mapping_table_memory * factor / MB))
        log_msg("estimated dftl memory footprint for 1TB: %d MB" % (reference_mapping_table_memory * factor / MB))
        log_msg("")

        # storage = float(reference_mapping_table_memory / 4.0 * page_size)
        # factor = 0.25*TB/storage

        # log_msg("estimated %s memory footprint for 0.25TB: %d MB" % (ftl_type, mapping_table_memory * factor / MB))
        # log_msg("estimated dftl memory footprint for 0.25TB: %d MB" % (reference_mapping_table_memory * factor / MB))
        # log_msg("")
    
