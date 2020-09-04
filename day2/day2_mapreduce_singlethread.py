from collections import defaultdict
import re
import time

def read_file():
    """打开txt文件"""
    f = open('hamlet.txt', 'r')
    return f


def mapping(fd):
    temp_list = []
    for line in fd.readlines():  # 按行读取文本
        ss = re.sub(re.compile(r"[-:_!?',;.]"), '', line.strip()).split(
            ' ')  # 我们读的每一句话都按空格分开，strip()避免输入的 字符串有回车，制表符等隐含字符
        for word in ss:
            word = word.strip(',.:;?!-|').strip('"').lower()  # 去掉单词开头末尾的标点符号
            temp_list.append([word, 1])  # 这个单词就是key，当这个单词被我观察到一次，就加一个1
    return temp_list


def shuffle(temp_list):
    word_dict = defaultdict(list)  # 使用list对word出现的频次进行记录
    for word, num in temp_list:
        word_dict[word].append(1)
    return word_dict


def reduce(worddict):
    res_dict = dict()  # 将结果存放在字典中（hashtable）
    for key, value in worddict.items():
        if key not in res_dict:
            res_dict[key] = sum(value)  # 对每个单词的词频列表求和，统计总的出现次数
    res_list = sorted(res_dict.items(), key=lambda item: item[1], reverse=False)  # 按照出现的频次进行排序
    return res_list


def print_res(res_list):
    for word, num in res_list:
        print('单词：' + word + ' 出现的次数为：' + str(num))


if __name__ == '__main__':
    fd = read_file()
    temp_list = mapping(fd)
    start = time.time()
    word_dict = shuffle(temp_list)
    res_list = reduce(word_dict)
    print_res(res_list)
    print("总耗时"+str(time.time()-start))
