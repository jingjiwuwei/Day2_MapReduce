# _*_coding:utf-8_*_

from collections import defaultdict
import re
import time, threading

# 全局变量 格式为：{’word‘:[1,1,1,1,1]}的形式，每个线程独立的向其中对应的单词键值中插入1
word_dict = defaultdict(list)

'''
Reader类，继承threading.Thread
@__init__方法初始化
@run方法实现了读文件的操作
'''


# 分块读取文件，并且进行map+shuffle操作
class MapReduce(threading.Thread):
    def __init__(self, file_name, start_pos, end_pos):
        super(MapReduce, self).__init__()
        self.file_name = file_name
        self.start_pos = start_pos
        self.end_pos = end_pos

    def run(self):
        fd = open(self.file_name, 'r')
        '''
        该if块主要判断分块后的文件块的首位置是不是行首，
        是行首的话，不做处理
        否则，将文件块的首位置定位到下一行的行首
        '''
        if self.start_pos != 0:
            fd.seek(self.start_pos - 1)
            if fd.read(1) != '\n':
                line = fd.readline()
                self.start_pos = fd.tell()
        fd.seek(self.start_pos)
        '''
        对文件块进行mapping处理
        '''
        while (self.start_pos <= self.end_pos):
            temp_list = self.mapping(fd)
            self.shuffle(temp_list)
            self.start_pos = fd.tell()
        # res_list = self.reduce()
        # res.append(res_list)

    # mapping
    def mapping(self, fd):
        temp_list = []
        for line in fd.readlines():  # 按行读取文本
            ss = re.sub(re.compile(r"[-:_!?',;.]"), '', line.strip()).split(
                ' ')  # 我们读的每一句话都按空格分开，strip()避免输入的 字符串有回车，制表符等隐含字符
            for word in ss:
                word = word.lower()  # 全部小写处理
                # word_dict[word].append(1)  # 这个单词就是key，当这个单词被我观察到一次，就加一个1 {'the':[1,1,1,1,1,1]}
                temp_list.append([word, 1])
        return temp_list

    # shuffle
    def shuffle(self, temp_list):
        for word, num in temp_list:
            word_dict[word].append(1)


'''
对文件进行分块，文件块的数量和线程数量一致
'''


class Partition(object):
    def __init__(self, file_name, thread_num):
        self.file_name = file_name
        self.block_num = thread_num

    def part(self):
        fd = open(self.file_name, 'r')
        fd.seek(0, 2)
        pos_list = []
        file_size = fd.tell()
        block_size = file_size // self.block_num
        start_pos = 0
        for i in range(self.block_num):
            if i == self.block_num - 1:
                end_pos = file_size - 1
                pos_list.append((start_pos, end_pos))
                break
            end_pos = start_pos + block_size - 1
            if end_pos >= file_size:
                end_pos = file_size - 1
            if start_pos >= file_size:
                break
            pos_list.append((start_pos, end_pos))
            start_pos = end_pos + 1
        fd.close()
        return pos_list


# reduce方法 (最后一步将word_list {’the‘:[1,1,1,1,1,1,1]}中的所有列表部分求和计算最后的词频)
def reduce(worddict):
    res_dict = dict()  # 将结果存放在字典中（hashtable）
    for key, value in worddict.items():
        if key not in res_dict:
            res_dict[key] = sum(value)  # 对每个单词的词频列表求和，统计总的出现次数
    res_list = sorted(res_dict.items(), key=lambda item: item[1], reverse=True)  # 按照出现的频次进行排序
    print("================打印出单词出现的词频高于10次的情况（从多到少）====================================")
    for word, fren in res_list:
        if fren > 100:
            print("单词 " + word + "出现的频次为：" + str(fren))
    return res_list


# 生成并开启线程
def gen_threading(file, threadnum, pos):
    reader_list = []
    for i in range(threadnum):
        reader = MapReduce(file, *pos[i])
        reader.start()
        reader_list.append(reader)
        print("线程" + str(i) + "已开启")
    for reader in reader_list:
        reader.join()


if __name__ == '__main__':
    file_name = 'hamlet.txt'  # 文件名
    thread_num = 3  # 在此改变线程数量
    pos = Partition(file_name, thread_num).part()  # 对txt文件进行分块处理
    start_time = time.time()  # 起始时间
    gen_threading(file_name, thread_num, pos)  # 生成并开启线程
    res = reduce(word_dict)
    print("==============================================================================")
    print("在线程数为 " + str(thread_num) + "的前提下：")
    print("时间消耗情况为 %f" % (time.time() - start_time))

    """实验发现多线程并没有比单线程快"""
    """原因：计时的时候把对文章分块的部分也算在内，同时由于文件大小比较下，多线程的优势没有发挥出来"""
