class CRB:
    HEAD = None
    def __init__(self, list):
        self.items = list

    def search(self, item):
        index = len(self.items)//2

        tail = float('inf')
        while(tail > item):
            index -= 1
            while(index >= 0 and self.items[index-1] != None):
                index -= 1
            tail = self.items[index-2]

        index = len(self.items)//2
        head = -1
        while(head < item):
            index += 1
            while(index < len(self.items) and self.items[index+1] != None):
                index += 1
            head = self.items[index+2]

        print(tail, head)


crb = CRB([0,1,2,10,None,4,5,8,13,None,6,7,17,18,None])
crb.search(7)