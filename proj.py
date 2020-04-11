import json
from mpi4py import MPI
from datetime import datetime
from collections import Counter
import os

# count = len(file.readlines())
# offset = length () / size

# path = "/Users/linyuming/Desktop/test.json"
# path = "/Users/linyuming/Desktop/smallTwitter.json"
path = "/home/yumingl/bigTwitter.json"

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


languageDict = {'am': 'Amharic', 'hu': 'Hungarian', 'pt': 'Portuguese', 'ar': 'Arabic',
                'is': 'Icelandic', 'ro': 'Romanian', 'hy': 'Armenian', 'in': 'Indonesian',
                'ru': 'Russian', 'bn': 'Bengali', 'it': 'Italian', 'sr': 'Serbian',
                'bg': 'Bulgarian', 'ja': 'Japanese', 'sd': 'Sindhi', 'my': 'Burmese',
                'kn': 'Kannada', 'si': 'Sinhala', 'zh': 'Chinese', 'km': 'Khmer',
                'sk': 'Slovak', 'cs': 'Czech', 'ko': 'Korean', 'sl': 'Slovenian',
                'da': 'Danish', 'lo': 'Lao', 'ckb': 'Sorani Kurdish', 'nl': 'Dutch',
                'lv': 'Latvian', 'es': 'Spanish', 'en': 'English', 'lt': 'Lithuanian',
                'sv': 'Swedish', 'et': 'Estonian', 'ml': 'Malayalam', 'tl': 'Tagalog',
                'fi': 'Finnish', 'dv': 'Maldivian', 'ta': 'Tamil', 'fr': 'French',
                'mr': 'Marathi', 'te': 'Telugu', 'ka': 'Georgian', 'ne': 'Nepali',
                'th': 'Thai', 'de': 'German', 'no': 'Norwegian', 'bo': 'Tibetan',
                'el': 'Greek', 'or': 'Oriya', 'tr': 'Turkish', 'gu': 'Gujarati',
                'pa': 'Panjabi', 'uk': 'Ukrainian', 'ht': 'Haitian', 'ps': 'Pashto',
                'ur': 'Urdu', 'iw': 'Hebrew', 'fa': 'Persian', 'ug': 'Uyghur',
                'hi': 'Hindi', 'pl': 'Polish', 'vi': 'Vietnamese', 'cy': 'Welsh',
                'und': 'Undefined'}


def fileSplit():
    # 返回文件大小，如果文件不存在就返回错误
    fileSize = os.path.getsize(path)
    # 单个文件按照总线程数量分
    singleSize = fileSize // size
    # 生成一个list，里面包含了每段的文件大小，[0,100,200,300,400,500]
    offsetList = []
    for i in range(size):
        offsetList.append(i * singleSize)
    offsetList.append(fileSize)
    return offsetList


def stasticData(jsonLine, hashtag, language):
    # 如果存在hashtag这个标签，判断是否在字典中有key存在。然后计数，如不存在则设为初始值1，如果存在，则对应value统计+1
    if jsonLine['doc']['entities']['hashtags'] != []:
        tagElem1 = jsonLine['doc']['entities']['hashtags']
        for i in range(0,len(tagElem1)):
            if tagElem1[i]['text'].lower() not in hashtag.keys():
                hashtag[tagElem1[i]['text'].lower()] = 1
            else:
                hashtag[tagElem1[i]['text'].lower()] += 1
    # 判断语言
    if jsonLine['doc']['metadata']['iso_language_code'] != []:
        tagElem2 = jsonLine['doc']['metadata']['iso_language_code']
        if tagElem2.lower() not in language.keys():
            language[tagElem2.lower()] = 1
        else:
            language[tagElem2.lower()] += 1
    # 判断转发
    if 'retweeted_status' in jsonLine['doc']:
        tagElem3 = jsonLine['doc']['retweeted_status']['entities']['hashtags']
        if tagElem3 != []:
            for i in range(0, len(tagElem3)):
                if tagElem3[i]['text'].lower() not in hashtag.keys():
                    hashtag[tagElem3[i]['text'].lower()] = 1
                else:
                    hashtag[tagElem3[i]['text'].lower()] += 1
    # 判断引用
    if 'quoted_status' in jsonLine['doc']:
        if jsonLine['doc']['quoted_status']['entities']['hashtags'] != []:
            tagElem4 = jsonLine['doc']['quoted_status']['entities']['hashtags']
            for i in range(0,len(tagElem4)):
                if tagElem4[i]['text'].lower() not in hashtag.keys():
                    hashtag[tagElem4[i]['text'].lower()] = 1
                else:
                    hashtag[tagElem4[i]['text'].lower()] += 1
    #判断转发中的引用
    if 'retweeted_status' in jsonLine['doc']:
        if 'quoted_status' in jsonLine['doc']['retweeted_status']:
            if jsonLine['doc']['retweeted_status']['quoted_stutes']['entities']['hashtags'] != []:
                tagElem5 = jsonLine['doc']['retweeted_status']['quoted_stutes']['entities']['hashtags']
                for i in range(0,len(tagElem5)):
                    if tagElem5[i]['text'].lower() not in hashtag.keys():
                        hashtag[tagElem5[i]['text'].lower()] = 1
                    else:
                        hashtag[tagElem5[i]['text'].lower()] += 1

    return hashtag, language


def process(offsetList):
    twitterHashtag = {}
    twitterLanguage = {}
    # 使用字典来存储 {hashtag:出现次数},{language:出现次数}
    with open(path, 'r') as file:
        # 定位当前rank的初始指针位置
        startPointer = offsetList[rank]
        # 定位当前rank的结束指针位置
        endPointer = offsetList[rank + 1]
        print('-------------------------')
        print('Rank %d start at: %d' % (rank, startPointer))
        print('Rank %d  end  at: %d' % (rank, endPointer))
        # 将指针移动到开始处
        file.seek(startPointer)
        while startPointer < endPointer:
            line = file.readline()
            # 更新初始指针
            startPointer = file.tell()
            #如果满足改行格式，则统计改行的数据
            try:
                if line.startswith('{"id'):
                    if line.endswith('},\n'):
                        fileData = json.loads(line[:-2])
                        twitterHashtag, twitterLanguage = stasticData(fileData, twitterHashtag, twitterLanguage)
                    else:
                        fileData = json.loads(line[:-1])
                        twitterHashtag, twitterLanguage = stasticData(fileData, twitterHashtag, twitterLanguage)
            except Exception:
                continue
    return twitterHashtag, twitterLanguage


def ranking(hashtag, language):
    hashtagCount = Counter(hashtag)
    hashtagTop10 = hashtagCount.most_common(10)
    languageCount = Counter(language)
    languageTop10 = languageCount.most_common(10)
    return hashtagTop10, languageTop10


def gatherResult(hashtagTop10, languageTop10):
    # 收集各rank的数据，收集格式为一个list嵌套，即[[XX,xx,XX]]
    gatherHashtag = comm.gather(hashtagTop10, root=0)
    gatherLanguage = comm.gather(languageTop10, root=0)

    # 如果是0号线程
    if rank == 0:
        reverseHashtag = []
        reverseLanguage = []

        # 反转数据并倒序排序
        for i in range(0, len(gatherHashtag)):
            for item in gatherHashtag[i]:
                reverseHashtag.append(item)

        for i in range(0, len(gatherLanguage)):
            for item in gatherLanguage[i]:
                reverseLanguage.append(item)

        # 重新统计各组数据
        resultHashtag = {}
        resultLanguage = {}
        for item in reverseHashtag:
            if item[0] not in resultHashtag.keys():
                resultHashtag[item[0]] = item[1]
            else:
                resultHashtag[item[0]] = item[1] + resultHashtag.get(item[0])

        for item in reverseLanguage:
            if item[0] not in resultLanguage.keys():
                resultLanguage[item[0]] = item[1]
            else:
                resultLanguage[item[0]] = item[1] + resultLanguage.get(item[0])

        # 给字典降序排一下序
        resultHashtag = sorted(resultHashtag.items(), key=lambda x: x[1], reverse=True)
        resultLanguage = sorted(resultLanguage.items(), key=lambda x: x[1], reverse=True)

        # 输出结果
        print('Rank %d, gather result and do sort:' % rank)
        print('-------------------------')
        for i in range(0, 10):
            print('%d.#%s,%s' % (i + 1, resultHashtag[i][0], resultHashtag[i][1]))
        print('-------------------------')
        for i in range(0, 10):
            print('%d.%s(%s),%s' % (
                i + 1, languageDict.get(resultLanguage[i][0]), resultLanguage[i][0], resultLanguage[i][1]))
        print('-------------------------')


if __name__ == '__main__':
    line_index = 0
    start = datetime.now()
    offsetList = fileSplit()
    twitterHashtag, twitterLanguage = process(offsetList)
    a, b = ranking(twitterHashtag, twitterLanguage)
    gatherResult(a, b)
    end = datetime.now()
    print('Rank %d Program Running Time: %s Seconds' % (rank, (end - start)))
