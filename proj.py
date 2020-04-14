import json
from mpi4py import MPI
from datetime import datetime
from collections import Counter
import os

path = "/home/yumingl/bigTwitter.json"

#initial MPI, and record time
initMpiStart = datetime.now()
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
initMpiEnd = datetime.now()

# a language dict
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

# split the file in to chunks
def fileSplit():
    # return the filesize
    fileSize = os.path.getsize(path)
    # divide the file with rank sum num
    singleSize = fileSize // size
    # generate a list with offsets，for example:[0,100,200,300,400,500]
    offsetList = []
    for i in range(size):
        offsetList.append(i * singleSize)
    offsetList.append(fileSize)
    return offsetList

# fliter the data in json that we need
def stasticData(jsonLine, hashtag, language):
    tagElem1 = jsonLine['doc']['entities']['hashtags']
    for i in range(0, len(tagElem1)):
        hashtag.append(tagElem1[i]['text'].lower())

    language.append(jsonLine['doc']['metadata']['iso_language_code'])

    if 'quoted_status' in jsonLine['doc']:
        tagElem3 = jsonLine['doc']['quoted_status']['entities']['hashtags']
        for i in range(0, len(tagElem3)):
            hashtag.append(tagElem3[i]['text'].lower())
        language.append(jsonLine['doc']['quoted_status']['metadata']['iso_language_code'])

    if 'retweeted_status' in jsonLine['doc']:

        tagElem2 = jsonLine['doc']['retweeted_status']['entities']['hashtags']
        for i in range(0, len(tagElem2)):
            hashtag.append(tagElem2[i]['text'].lower())
        language.append((jsonLine['doc']['retweeted_status']['metadata']['iso_language_code']))

        if 'quoted_status' in jsonLine['doc']['retweeted_status']:
            tagElem4 = jsonLine['doc']['retweeted_status']['quoted_status']['entities']['hashtags']
            for i in range(0, len(tagElem4)):
                hashtag.append(tagElem4[i]['text'].lower())
            language.append(jsonLine['doc']['retweeted_status']['quoted_status']['metadata']['iso_language_code'])

    return hashtag, language

# parallel read and process
def process():
    parallelStart = datetime.now()
    offsetList = fileSplit()
    twitterHashtag = []
    twitterLanguage = []
    with open(path, 'r') as file:
        # position current cursor start
        startPointer = offsetList[rank]
        # position the end point
        endPointer = offsetList[rank + 1]

        # move the cursor to the start point
        file.seek(startPointer)
        while startPointer < endPointer:
            # read each line
            line = file.readline()
            # record current cursor point
            startPointer = file.tell()
            # if the line is in json format
            if line.startswith('{"id'):
                if line.endswith('},\n'):
                    fileData = json.loads(line[:-2])
                    twitterHashtag, twitterLanguage = stasticData(fileData, twitterHashtag, twitterLanguage)
                else:
                    fileData = json.loads(line[:-1])
                    twitterHashtag, twitterLanguage = stasticData(fileData, twitterHashtag, twitterLanguage)
    parallelEnd = datetime.now()
    print('Rank %d parallel time: %s' % (rank, (parallelEnd-parallelStart)))
    return Counter(twitterHashtag), Counter(twitterLanguage)


def gatherResult(myHashtag, myLanguage):
    # record the mpi communication time
    mpiCommStart = datetime.now()
    gatherHashtag = comm.gather(myHashtag, root=0)
    gatherLanguage = comm.gather(myLanguage, root=0)
    mpiCommEnd = datetime.now()

    # record the serial gather time in rank 0
    serialStart = datetime.now()
    if rank == 0:
        totalHashtag = gatherHashtag[0]
        totalLanguage = gatherLanguage[0]

        for i in range(1,len(gatherHashtag)):
            totalHashtag += gatherHashtag[i]

        for i in range(1,len(gatherLanguage)):
            totalLanguage += gatherLanguage[i]

        #sorted the rank
        sortedTotalHashtag = Counter(totalHashtag).most_common()
        sortedTotalLanguage = Counter(totalLanguage).most_common()

        # deal with duplication
        duplicatedHashtagIndex, duplicatedLanguageIndex = 0, 0

        for i in range(11, len(sortedTotalHashtag)):
            if sortedTotalHashtag[i] == sortedTotalHashtag[10]:
                duplicatedHashtagIndex += 1
            else:
                break

        for i in range(11, len(sortedTotalLanguage)):
            if sortedTotalLanguage[i] == sortedTotalLanguage[10]:
                duplicatedLanguageIndex += 1
            else:
                break

        top10Hashtag = sortedTotalHashtag[:(10 + duplicatedHashtagIndex)]
        top10Language = sortedTotalLanguage[:(10 + duplicatedLanguageIndex)]

        serialEnd = datetime.now()

        #print result into output
        print('-------------------------')
        for i in range(0, 10 + duplicatedHashtagIndex):
            print('%d.#%s,%s' % (i + 1, top10Hashtag[i][0], top10Hashtag[i][1]))
        print('-------------------------')
        for i in range(0, 10 + duplicatedLanguageIndex):
            print('%d.%s(%s),%s' % (
            i + 1, languageDict.get(top10Language[i][0]), top10Language[i][0], top10Language[i][1]))
        print('-------------------------')

        print('Rank %d serial part time: %s seconds' % (rank, (serialEnd - serialStart)))
        print('Rank %d mpi communicate time: %s seconds' % (rank, (mpiCommEnd - mpiCommStart)))
    print('Rank %d init mpi time: %s Seconds' % (rank, (initMpiEnd - initMpiStart)))



if __name__ == '__main__':
    line_index = 0
    twitterHashtag, twitterLanguage = process()
    gatherResult(twitterHashtag, twitterLanguage)