#!/usr/bin/env python3
import random
import math

# how many LPN to PPN mapping entries a translation page could hold
# trans_page_entry = 512
bytes_PPN = 4  # how many bytes a PPN takes

class MetaInfo:
    def __init__(self, i, is_head=None, is_default=None):
        self.is_head = is_head
        self.is_default = is_default
        self.i = i

class SFTLPage:
    def __init__(self, index, trans_page_entry):
        self.trans_page_entry = trans_page_entry
        self.meta = [MetaInfo(i) for i in range(self.trans_page_entry)]
        # self.meta[0].is_head = True
        self.segment = []
        self.has_overwrite = None
        self.index = index
        self.mapping = dict()

    def __str__(self):
        return str(self.segment)

    def lpn_offset(self, lpn):
        return lpn % self.trans_page_entry

    def group_consecutives(self, vals, step=1):
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

    def group_mapping_into_entries(self):
        last_k = None
        groups_of_entries = []
        entries = []
        for k,v in self.mapping.items():
            if not last_k or k == last_k + 1:
                entries.append((k, v))
            else:
                groups_of_entries.append(entries)
                entries = []
                entries.append((k, v))
            last_k = k

        if len(entries) > 0:
            groups_of_entries.append(entries)
            entries = []

            
        return groups_of_entries
                

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
    def levels(self):
        return 1

    @property
    def segments(self):
        return []

    @property
    def memory(self):
        heads = [x.i for x in self.meta if x.is_head]
        if len(heads) == 0:
            return 0
        additional_heads = []
        for end_point in list(zip(*self.segment))[1]:
            if end_point+1 not in heads:
                additional_heads.append(end_point)
        
        # if self.has_overwrite:
        #     return bytes_PPN * len(heads+additional_heads) + trans_page_entry * 2 / 8. + bytes_PPN
        return bytes_PPN * len(heads+additional_heads) + self.trans_page_entry * 2 / 8. + bytes_PPN
        
    def update(self, entries, blocknum=-1):
        entries = [(self.lpn_offset(lpn), ppn) for lpn, ppn in entries]
        for lpn, ppn in entries:
            self.mapping[lpn] = ppn

        lpns = self.group_consecutives(list(list(zip(*entries))[0]))
        for n_lpn, size in lpns:
            self._write(n_lpn, size)


    def lookup(self, lpn, first=True):
        lpn = self.lpn_offset(lpn)
        if lpn in self.mapping:
            return [(self.mapping[lpn], True, None)], 1, 0, 0
        else:
            return [], 1, 0, 0

    def compact(self, promote=False):
        pass

    def merge(self, other):
        for entries in self.group_mapping_into_entries():
            other.update(entries)
        
        self.meta = other.meta
        self.segment = other.segment
        self.has_overwrite = other.has_overwrite

        for k,v in other.mapping.items():
            if k not in self.mapping:
                self.mapping[k] = v

    def promote(self):
        pass

    def gc(self):
        pass

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


class DFTLPage:
    def __init__(self, index):
        self.index = index
        self.mapping = dict()

    def __str__(self):
        return ""

    @property
    def levels(self):
        return 1

    @property
    def segments(self):
        return []

    @property
    def memory(self):
        return 2 * bytes_PPN * len(self.mapping)
        
    def update(self, entries, blocknum=-1):
        for lpn, ppn in entries:
            self.mapping[lpn] = ppn

    def lookup(self, lpn, first=True):
        if lpn in self.mapping:
            return [(self.mapping[lpn], True, None)], 1, 0, 0
        else:
            return [], 1, 0, 0

    def compact(self, promote=False):
        pass

    def merge(self, other):
        for k,v in other.mapping.items():
            if k not in self.mapping:
                self.mapping[k] = v

    def promote(self):
        pass

    def gc(self):
        pass

if __name__ == "__main__":
    
    trans_pages = dict()
    entries = [(i,i) for i in range(256)]
    page_no = 0 
    
    trans_pages = {i : SFTLPage(i) for i in range(2)}
    trans_pages[0].update(entries, 0)
    trans_pages[1].update(entries[:128], 1)
    trans_pages[0].merge(trans_pages[1])
    # print(trans_pages[singlewrite[0] // trans_page_entry])

    print("SFTL Memory consumption in Byte is:")
    print(sum([x.memory for x in trans_pages.values()]))
    print(len([x for x in trans_pages.values() if x.has_overwrite]) / float(len([x for x in trans_pages.values()])))
    page_no, page = list(trans_pages.keys())[0], list(trans_pages.values())[0]
    print(page_no)
    print(page.segment)
    # print([x.i for x in page.meta if x.is_head])
    # print(len(list(trans_pages.values())))