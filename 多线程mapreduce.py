

from __future__ import print_function, division, unicode_literals
import sys, re, time, os
import operator
from collections import Counter
from functools import reduce
from multiprocessing import Pool
from day2.utils import humansize


class WordCounter(object):
    def __init__(self, from_file, to_file=None, workers=1, coding='utf-8'):
        '''根据设定的进程数，把文件from_file分割成大小基本相同，数量等同与进程数的文件段，
        来读取并统计词频，然后把结果写入to_file中，当其为None时直接打印在终端或命令行上。
        Args:
        @from_file 要读取的文件
        @to_file 结果要写入的文件
        @workers 进程数，为1时按for line in open(xxx)读取；>=2时为多进程分段读取；默认为根据文件大小选择0或cpu数量的64倍
        @coding 文件的编码方式，默认为采用chardet模块读取前1万个字符才自动判断
        @max_direct_read_size 直接读取的最大值，默认为10000000（约10M）
        How to use:
        w = WordCounter('a.txt', 'b.txt')
        w.run()
        '''
        if not os.path.isfile(from_file):
            raise Exception('No such file: 文件不存在')
        self.f1 = from_file
        self.filesize = os.path.getsize(from_file)
        self.f2 = to_file
        self.workers = int(workers)
        self.coding = coding
        self._c = Counter()

    def map_reduce(self):
        """
        共有单线程和多线程两种运行方法，单线程： worker:1 运行的是count_single()方法
                                  多线程： worker>2 运行的是count_multi()方法
        :return: 将得到的单词和对应的词频保存到指定的txt文件中
        """
        per_block_size = self.filesize // self.workers

        if self.workers == 1:
            start = time.time()
            self.count_single(self.f1, self.filesize)
        else:
            # 多线程 分块读取文件 分别统计词频
            pool = Pool(self.workers)  # 线程池
            res_list = []
            start = time.time()
            for i in range(self.workers):
                p1 = per_block_size * i
                p2 = per_block_size * (i + 1)
                args = [self, self.f1, p1, p2, self.filesize]  # 分块读取文件
                res = pool.apply_async(func=self.wrap, args=args)
                print("线程" + str(i) + " 已经结束")
                res_list.append(res)
            pool.close()
            pool.join()
            self._c.update(reduce(operator.add, [r.get() for r in res_list]))
        cost = '{:.1f}'.format(time.time() - start)
        if self.f2:
            with open(self.f2, 'wb') as f:
                f.write(self.result.encode(self.coding))
        # print(self.result)
        size = humansize(self.filesize)
        tip = '\n文件大小: {}. 线程数: {}. 总耗时: {} 秒'
        print(tip.format(size, self.workers, cost))
        self.cost = cost + 's'

    def count_single(self, from_file, f_size):
        '''
        单进程读取文件并统计词频
        '''
        with open(from_file, 'rb') as f:
            for line in f.readlines():
                self._c.update(self.parse(line))

    def count_multi(self, fn, p1, p2, f_size):
        """多线程读取文件并统计词频"""
        c = Counter()
        with open(fn, 'rb') as f:
            if p1:  # 为防止字被截断的，分段处所在行不处理，从下一行开始正式处理
                f.seek(p1 - 1)
                while b'\n' not in f.read(1):
                    pass
            while 1:
                line = f.readline()
                c.update(self.parse(line))
                pos = f.tell()
                if pos >= p2:
                    return c

    # 解析读取的文件流，去掉单词开头末尾的标点符号
    def parse(self, line):
        """
        :param line: 逐行读取文件
        :return: collection.Counter 计数器
        """
        new_string = re.sub(re.compile(r"[-:_!?',;.]"), '', line.decode(self.coding))
        word_counter = Counter(new_string.lower().split(' '))
        return word_counter

    def flush(self):  # 清空统计结果
        self._c = Counter()

    @property
    def counter(self):  # 返回统计结果的Counter类
        return self._c

    @property
    def result(self):  # 返回统计结果的字符串型式，等同于要写入结果文件的内容
        ss = ['{}: {}'.format(i, j) for i, j in self._c.most_common()]
        return '\n'.join(ss)

    def wrap(self, wcounter, fn, p1, p2, f_size):
        return wcounter.count_multi(fn, p1, p2, f_size)


def run(from_file, to_file, args):
    """

    :param from_file: 需要读取的文件
    :param to_file:   结果保存到指定文件
    :param args:
    :return:
    """
    for i in sys.argv:
        for k in args:
            if re.search(r'{}=(.+)'.format(k), i):
                args[k] = re.findall(r'{}=(.+)'.format(k), i)[0]

    w = WordCounter(from_file, to_file, **args)
    w.map_reduce()


if __name__ == '__main__':
    from_file = 'hamlet.txt'
    to_file_multi = './result_multi.txt'
    to_file_single = "./result_single.txt"
    args_multi = {'coding': 'utf-8', 'workers': 5}  # workers代表线程的数量
    args_single = {'coding': 'utf-8', 'workers': 1}
    print('============================================多线程===================================================')

    # 多线程读取文件，并分别统计词频相加
    run(from_file, to_file_multi, args_multi)
    print('============================================单线程===================================================')
    # 单线程读取文件，并统计词频
    run(from_file, to_file_single, args_single)

"""hamlet文件比较小，我选了一个大小为29M的txt文档，比较容易对比出结果,以下是输出结果"""
"""文件本身过小的话，多线程中大量时间被消耗在文件的读取上，反而没有单线程快"""
# ============================================多线程===================================================
# 线程0 已经结束
# 线程1 已经结束
# 线程2 已经结束
# 线程3 已经结束
# 线程4 已经结束
#
# 文件大小: 29 M. 线程数: 5. 总耗时: 4.1 秒
# ============================================单线程===================================================
#
# 文件大小: 29 M. 线程数: 1. 总耗时: 7.3 秒


