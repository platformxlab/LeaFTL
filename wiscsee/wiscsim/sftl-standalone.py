#!/usr/bin/env python3
import random
import math
from datacache import *
from collections import defaultdict
SSD_capacity = 33554432  # how many pages are there in a SSD
# how many LPN to PPN mapping entries a translation page could hold
trans_page_entry = 2048
bytes_PPN = 4  # how many bytes a PPN takes
page_size = 8192  # page size in byte

# def parse_events(filename, lineno=float('inf'), format="MSR"):
#     if "rocksdb" in filename:
#         format = "blktrace"
#     if "systor17" in filename:
#         format = "systor"
#     if "traces" in filename:
#         format = "normal"
#     if "FIU" in filename:
#         format = "FIU"

#     events = []
#     # Dict<Format, Tuple<size_scale, time_scale, delimeter>>
#     format_config = {"MSR" : (1, 100, ","), "blktrace" : (512, 1000**3, " "), "systor" : (1, 1000**3, ","), "normal" : (1, 1000, " "), "FIU" : (512, 1, " ")} 
#     size_scale = format_config[format][0]
#     delimeter = format_config[format][2]

#     with open(filename) as fp:
#         for i, raw in enumerate(fp):
#             if i % 1000000 == 0:
#                 print("parsed %d lines" % i)
#             if i > lineno:
#                 break
#             # parse trace
#             line = raw.strip().split(delimeter)
#             if format == "MSR":
#                 _, _, _, mode, offset, size, _ = line
#                 offset, size = int(offset), int(size)
#             elif format == "blktrace":
#                 line = list(filter(lambda _: _ != '', line))
#                 if len(line) != 11:
#                     continue
#                 d, cpu, record, t, pid, action, mode, offset, _, size, pname = line
#                 t, offset, size = float(t), int(offset), int(size)
#                 if action != 'Q':
#                     continue
#             elif format == "normal":
#                 t, d, offset, size, mode = line
#                 t, d, offset, size, mode = int(t), int(d), int(offset), int(size), int(mode)
#             elif format == "systor":
#                 if i == 0:
#                     continue
#                 _, _, mode, _, offset, size = line
#                 offset, size = int(offset), int(size)
#             elif format == "FIU":
#                 t, pid, proc, offset, size, mode, _, d, _ = line
#                 t, offset, size = int(t), int(offset), int(size)

#             offset *= size_scale
#             size *= size_scale
#             if mode in ["Write", "W", 1, 'WS']:
#                 events.append((offset // page_size, math.ceil(size / page_size)))

#     return events

def split_lpns(offset, size):
    lpns = [lpn for lpn in range(int(math.floor(offset/float(page_size))), int(math.ceil((offset+size)/float(page_size))))]

    return lpns

def parse_events(filename, lineno=float('inf'), format="MSR"):
    if "rocksdb" in filename:
        format = "blktrace"
    if "systor17" in filename:
        format = "systor"
    if "traces" in filename:
        format = "normal"
    if "FIU" in filename:
        format = "FIU"
    if "Financial" in filename:
        format = "Financial"

    # Dict<Format, Tuple<size_scale, time_scale, delimeter>>
    format_config = {"MSR" : (1, 100, ","), "blktrace" : (512, 1000**3, " "), "systor" : (1, 1000**3, ","), "normal" : (1, 1000, " "), "FIU" : (512, 1, " "), "Financial" : (1, 1000**3, ",", 512)} 
    size_scale = format_config[format][0]
    offset_scale = size_scale
    time_scale = format_config[format][1]
    delimeter = format_config[format][2]
    if len(format_config[format]) > 3:
        offset_scale = format_config[format][3]


    with open(filename) as fp:
        t_start = None
        last_t = 0
        active_events = 0
        exist_lpns = dict()
        warm_up_writes = []
        events = []
        for i, raw in enumerate(fp):

            # parse trace
            line = raw.strip().split(delimeter)
            if format == "MSR":
                t, p, d, mode, offset, size, t0 = line
                t, d, offset, size, t0 = int(t), int(d), int(offset), int(size), int(t0)
            if format == "normal":
                t, d, offset, size, mode = line
                t, d, offset, size, mode = int(t), int(d), int(offset), int(size), int(mode)
            elif format == "blktrace":
                line = filter(lambda _: _ != '', line)
                raise NotImplementedError
            elif format == "systor":
                if i == 0:
                    continue
                t, t0, mode, d, offset, size = line
                if t0 == "":
                    t0 = 0.0
                t, d, offset, size, t0 = float(t), int(d), int(offset), int(size), float(t0)
            elif format == "Financial":
                app, offset, size, mode, t = line
                if int(app)!=0:
                    continue
                t, offset, size = float(t), int(offset), int(size)
            elif format == "FIU":
                t, pid, proc, offset, size, mode, _, d, _ = line
                t, offset, size = float(t), int(offset), int(size)

            # shift timestamp
            if not t_start:
                t_start = t
            t -= t_start

            # scale trace
            offset *= offset_scale
            size *= size_scale
            t = int(t*time_scale)
            if size == 0:
                continue

            if mode in ["Read", "R", 0, 'r']:
                should_warm_up = False
                for lpn in split_lpns(offset, size):
                    if lpn not in exist_lpns:
                        should_warm_up = True
                        exist_lpns[lpn] = None
                if should_warm_up:
                    warm_up_writes += [(offset // page_size, math.ceil(size / page_size))]
            elif mode in ["Write", "W", 1, 'w']:
                for lpn in split_lpns(offset, size):
                    exist_lpns[lpn] = None

            # create event
            if t < last_t:
                continue
            # events += [ControlEvent(OP_SLEEP, arg1=t - last_t)]
            events += [(offset // page_size, math.ceil(size / page_size))]
            active_events += 1
            last_t = t
        
            # termination
            if i > lineno:
                break
    
    # timestamp from traces might not be sorted! (now we abort the unsorted ones)
    # events = sorted(events, key=lambda event: event.timestamp)
    # for i in range(0, len(events)):
    #     events.insert(i*2, ControlEvent(OP_SLEEP, arg1=None))
    # last_t = 0
    # for i in range(0, len(events), 2):
    #     sleep, event = events[i], events[i+1] 
    #     sleep.arg1 = event.timestamp - last_t
    #     last_t = event.timestamp

    events = warm_up_writes + events
    return events

def split_write_to_page(start_lpn, size):
    writes = []
    pages_to_write = size
    current_write_point = start_lpn
    while pages_to_write > 0:
        current_write_size = min(pages_to_write, trans_page_entry *
                                 (current_write_point // trans_page_entry) - current_write_point + trans_page_entry)
        writes.append((current_write_point, current_write_size))
        pages_to_write = pages_to_write - current_write_size
        current_write_point = current_write_size + current_write_point
    return writes


class meta_info:
    def __init__(self, i, is_head=None, is_default=None):
        self.is_head = is_head
        self.is_default = is_default
        self.i = i


class trans_page:
    def __init__(self, index):
        self.meta = [meta_info(i) for i in range(trans_page_entry)]
        # self.meta[0].is_head = True
        self.segment = []
        self.has_overwrite = None
        self.index = index

    def __str__(self):
        # return \
        #     str([x[0] for x in enumerate(self.meta) if x[1].is_head and x[1].is_default]) + "\n" + \
        #     str([x[0] for x in enumerate(self.meta)
        #          if x[1].is_head and not x[1].is_default])
        return str(self.segment)
    # def split_write_to_segment(self, n_start_lpn, size):
    #     writes = []
    #     pages_to_write = size
    #     current_write_point = n_start_lpn
    #     current_segment = self.find_in_segment(n_start_lpn)
    #     while pages_to_write > 0:
    #         if pages_to_write <= (self.segment[current_segment][1] - current_write_point + 1):
    #             current_write_size = pages_to_write
    #         else:
    #             current_write_size = self.segment[current_segment][1] - \
    #                 current_write_point + 1
    #             current_segment = current_segment + 1
    #         writes.append((current_write_point, current_write_size))
    #         pages_to_write = pages_to_write - current_write_size
    #         current_write_point = current_write_size + current_write_point
    #     return writes

    def find_in_segment(self, n_lpn):
        left_pointer = 0
        right_pointer = len(self.segment) - 1
        find = False
        while left_pointer <= right_pointer:
            cur_pointer = (left_pointer + right_pointer) // 2
            if self.segment[cur_pointer][0] > n_lpn:
                right_pointer = cur_pointer - 1
            elif self.segment[cur_pointer][1] < n_lpn:
                left_pointer = cur_pointer + 1
            else:
                find = True
                break
        if not find:
            return -1
        return cur_pointer

    @property
    def memory_size(self):
        heads = [x.i for x in self.meta if x.is_head]
        additional_heads = []
        for end_point in list(zip(*self.segment))[1]:
            if end_point+1 not in heads:
                additional_heads.append(end_point)
        if self.has_overwrite:
            return bytes_PPN * len(heads+additional_heads) + trans_page_entry * 2 / 8. + bytes_PPN
        return bytes_PPN * len(heads+additional_heads) + trans_page_entry * 2 / 8. + bytes_PPN
        

    # def split(self, left_split, n_lpn, left_segment=None):
    #     if not left_split:
    #         segment_no = self.find_in_segment(n_lpn)
    #     else:
    #         segment_no = left_segment
    #     segment_begin = self.segment[segment_no][0]
    #     segment_end = self.segment[segment_no][1]
    #     if not left_split:
    #         new_segment_end = n_lpn
    #         new_segment_begin = n_lpn + 1
    #     else:
    #         new_segment_end = n_lpn - 1
    #         new_segment_begin = n_lpn
    #     for i in range(n_lpn + 1, segment_end + 1):
    #         if self.meta[i].is_default:
    #             self.meta[i].is_head = True
    #             break
    #     self.segment[segment_no] = (segment_begin, new_segment_end)
    #     self.segment.insert(segment_no + 1, (new_segment_begin, segment_end))

    def write(self, n_lpn, size):
        # for singlewrite in self.split_write_to_segment(n_lpn, size):
        self._write(n_lpn, size)
        # bitmap = 0
        # has_overwrite = False
        # for segment in self.segment:
        #     # bitmap = bitmap + segment[1] - segment[0] + 1
        #     if all([x.is_default is True for x in self.meta[segment[0]:segment[1] + 1]]):
        #         pass
        #     else:
        #         has_overwrite = True
        #     # else:
        #     #     bitmap = bitmap + (segment[1] - segment[0] + 1) * 2
        # if has_overwrite:
        #     self.has_overwrite = has_overwrite

    def _write(self, n_lpn, size):
        left_segment = self.find_in_segment(n_lpn)
        right_segment = self.find_in_segment(n_lpn + size - 1)
        overwrite = (left_segment == right_segment) and (
            left_segment != -1) and (n_lpn != self.segment[left_segment][0]) and (n_lpn + size - 1 != self.segment[left_segment][1])
        if overwrite:
            self.meta[n_lpn].is_default, self.meta[n_lpn].is_head = False, True
            for i in range(n_lpn + 1, n_lpn + size):
                self.meta[i].is_default, self.meta[i].is_head = False, False
            if not self.meta[n_lpn + size].is_default:
                self.meta[n_lpn + size].is_head = True
            self.has_overwrite = True

        else:
            self.meta[n_lpn].is_default, self.meta[n_lpn].is_head = True, True
            for i in range(n_lpn + 1, n_lpn + size):
                self.meta[i].is_default, self.meta[i].is_head = True, False
        
            if (right_segment != -1):
                initial_segment_end = self.segment[right_segment][1]
            if (left_segment != -1) and (n_lpn != self.segment[left_segment][0]):
                self.segment[left_segment] = (
                    self.segment[left_segment][0], n_lpn - 1)
            if (right_segment != -1) and (n_lpn + size - 1 != initial_segment_end):
                self.segment[right_segment] = (
                    n_lpn + size, initial_segment_end)
            insert_position = len([x for x in self.segment if x[1] < n_lpn])
            elements_to_delete = len([x for x in self.segment if (
                x[1] <= n_lpn + size - 1) and (x[0] >= n_lpn)])
            self.segment.insert(insert_position, (n_lpn, n_lpn + size - 1))
            del self.segment[insert_position +
                             1:insert_position + 1 + elements_to_delete]
        # default_head_removed = False
        # # left_segment = self.find_in_segment(n_lpn)
        # # right_segment = self.find_in_segment(n_lpn + size - 1)
        # for i in range(n_lpn, n_lpn + size):
        #     if self.meta[i].is_default and self.meta[i].is_head:
        #         default_head_removed = True
        #     if i != n_lpn:
        #         self.meta[i].is_head = False
        #     else:
        #         self.meta[i].is_head = True
        #     self.meta[i].is_default = False
        # #TODO:safe guard the length
        # # left_should_split = (not (self.meta[n_lpn].is_default)) and (
        # #     n_lpn != self.segment[left_segment][0])
        # # right_should_split = (not (self.meta[n_lpn + size - 1].is_default)) and (
        # #     n_lpn + size - 1 != self.segment[right_segment][1])
        # if default_head_removed:
        #     for i in range(n_lpn + size, trans_page_entry):
        #         if self.meta[i].is_default:
        #             self.meta[i].is_head = True
        #             break
        # try:
        #     if (not self.meta[n_lpn + size].is_default):
        #         self.meta[n_lpn + size].is_head = True
        # except:
        #     pass
        # if left_should_split:
        #     self.split(True, n_lpn, left_segment)
        # if right_should_split:
        #     self.split(False, n_lpn + size - 1)

def group_consecutives(vals, step=1):
    """Return list of consecutive lists of numbers from vals (number list)."""
    run = []
    results = [run]
    expect = None
    for v in vals:
        if (v == expect) or (expect is None):
            run.append(v)
        else:
            run = [v]
            results.append(run)
        expect = v + step
    
    for i, result in enumerate(results):
        results[i] = (result[0], len(result))

    return results