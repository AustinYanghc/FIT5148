#here starts the first half of task1
#first pick a partition method:
def rr_partition(data,n):
    """
        Perform a simple round robin partition on the given data set
        
        Parameters:
        data: the dataset to be partitioned, which is a list
        n: the number of groups that the dataset will be divided into
        
        Return:
        result: the partitioned subset of the dataset
        """
    result = []
    for i in  range(n):
        result.append([])
    for index,element in enumerate(data):
        index_bin = index%n
        result[index_bin].append(element)
    return result
#here starts task1 part2
def linear_seach(data,key):
    """
        Perform linear search on given dataset
        
        Parameters:
        dat: the dataset to be searched
        key: the key(can be a range) used for searching
        
        Return:
        result: a tuple containing the index of the matched record and the query result
        """
    position = -1
    found = None
    result = []
    for record in data:
        if int(record[-1]) in range(key[0],key[1]):
            found = record[:2] + [record[-3]]
            position = data.index(record)
            result.append(found)
    return result

def parallel_search_temperature(data,query,n_processor):
    """
        A method doing parallel search on a given dataset ,
        when given a search clue like a single key or a range for certain column value
        
        Parameters:
        data: the dataset to be searched, which is a list
        query: a query record
        n_processer: the number of processor to parallize the search job
        
        Return:
        results: the list of all search results in all processors
        """
    results = [['Latitude','Longitude','Confidence']]
    pool = Pool(processes=n_processor)
    datasets = rr_partition(data, n_processor)
    for partition in datasets:
        result = pool.apply_async(linear_seach, args=(partition,query))
        output = result.get()
        results += output
    return results





#import pandas to read file
import csv
from multiprocessing import Pool

# read file data to memeory
# fireData
fireData = []
with open('FireData.csv') as firePath:
    reader = csv.reader(firePath)
    for row in reader:
        fireData.append(row)
#climateData
climateData = []
with open('ClimateData.csv') as climatePath:
    reader = csv.reader(climatePath)
    for row in reader:
        climateData.append(row)
del fireData[0]
del climateData[0]
#
climateData
#
fireData
#
'''
Assume that we have 3 processors, as a result we can divde the data to 3 parts by using two time nodes. 
For divde equally, the time node shuold be the date of 10 and 20. If date less than 10 put it to first processor, else 
if it greater than 10 and less 20, put it to second processor, else put it to the last one.
'''
def task1_1_whichProcessor(date):

    if int(date[-2:]) <= 10:
        return 0
    else:
        if int(date[-2:]) <= 20:
            return 1
        else:
            return 2
        
'''
This is the algorithm how we partioning data, for each record in the target table, we take the date value and compare
with the time node. This function will back a value to piont the exactly which processor
'''

def task1_1_partionByTime(table):
    result = []
    
    for i in range(3):
        result.append([])
        

    for eachRecord in table:
        result[task1_1_whichProcessor(eachRecord[1])].append(eachRecord)
    return result


'''
We alread get a partioned data. When user input a date value, firstly we use "task1_1_whichProcessor" to determine
which worker has may have this value, then searh one by one in that worker. Everytime we find the value we put it to
result list.
'''

def serialSearh(table, date):
    for eachRecord in table:
        if eachRecord[1] == date:
            return eachRecord

def task1_1_searching(date, partionedTable):
    result = []
    workerTask = []
    pool = Pool(processes= 3)
    
    for i in partionedTable:
        workerTask = pool.apply_async(serialSearh, args=(i,date))
        output = workerTask.get()
        result.append(output)
    
    pool.close()
    return result

 #
task1_1_searching('2017-12-15',task1_1_partionByTime(climateData))
#
def task2_2_whichProcessor(date):

    if int(date[-2:]) <= 10:
        return 0
    else:
        if int(date[-2:]) <= 20:
            return 1
        else:
            return 2
        
#
def task2_2_partionByTime(climateData, fireData):
    result = []
    fireData = sorted(fireData, key= lambda fireData:fireData[-2])
    for i in range(3):
        result.append([])
        for j in range(2):
            result[i].append([])
        

    for eachRecord in climateData:
        result[task2_2_whichProcessor(eachRecord[1])][0].append(eachRecord)
        
    for eachRecord in fireData:
        result[task2_2_whichProcessor(eachRecord[6])][1].append(eachRecord)
    return result
#
def task2InnerJoin(table):
    result = []
    fireList = list (table[1])
    climateList = list(table[0])
    climateIndex = fireIndex = 0
    while True:
        climateDate = climateList[climateIndex][1]
        fireDate = fireList[fireIndex][-2]

        if climateDate > fireDate:
            fireIndex = fireIndex + 1
        else:
            if climateDate < fireDate:
                climateIndex = climateIndex + 1
            else:
                if int(fireList[fireIndex][5]) > 80 and int(fireList[fireIndex][5])<100:
                    result.append({",".join([str(fireList[fireIndex][3]),str(fireList[fireIndex][5]),str(fireList[fireIndex][2]),str(climateList[climateIndex][2])])})
                if climateList[climateIndex][1] == fireList[fireIndex+1][-2]:
                    fireIndex += 1
                else:
                    fireIndex += 1
                    climateIndex += 1
        if (climateIndex == len(climateList)-1) or (fireIndex == len(fireList)-1):
            break
    return result

def Task2_2(partionedTable):
    result = []
    local_result = []
    workerTask = []
    pool = Pool(processes= 3)
    
    for i in partionedTable:
        workerTask = pool.apply_async(task2InnerJoin, [i])
        output = workerTask.get()
        for eachJoining in output:
            result.append(eachJoining)
    pool.close()
    
            
            
            
    return result


#
task2InnerJoin(task2_2_partionByTime(climateData, fireData)[1])
#
Task2_2(task2_2_partionByTime(climateData, fireData))

#
def task3DataPartion(fireData):
    result = []
    for i in range(3):
        result.append([])
    
    for index, eachRecord in enumerate(fireData):
        index_bin = int(index%3)
        result[index_bin].append(eachRecord)
    
    return result
#
def quicksort(table):
    if len(table) <= 1:
        return table
    less = []
    greater = []
    baseRecord = table.pop()
    base = baseRecord[2]
    for x in table:
        if x[2] < base:
            less.append(x)
        else:
            greater.append(x)
    return quicksort(less) + [baseRecord] + quicksort(greater)
#
def findMin(table):
    minIndex = 0
    minNum = float(table[0][2])
    for index in range(len(table)):
        if float(table[index][2]) < minNum:
            minNum = float(table[index][2])
            minIndex = index
    return minIndex

def ThreeWay(SortdePartionedTable):
    indexes = []
    result = []
    T = []
    ifMax = ['max','max','1000000000']
    for i in range(len(SortdePartionedTable)):
        indexes.append(0)
    
    while(True):
        T = []
        for i in range(len(SortdePartionedTable)):
            
            if (indexes[i]>= len(SortdePartionedTable[i])):
                T.append(ifMax)
            else:
                T.append(SortdePartionedTable[i][indexes[i]])
        #print(T)
        
        smallest = findMin(T)
        smallestRecord = T[smallest]
    
        if(T[smallest] == ifMax):
            break
        
        result.append(smallestRecord)
        indexes[smallest] += 1
    return result
#
def paralleSort(partionedTable):
    result = []
    pool = Pool(processes=3)
    
    for eachList in partionedTable:
        workTask = pool.apply_async(quicksort, [eachList])
        result.append(workTask.get())
    pool.close()
    return result
#
ThreeWay(paralleSort(task3DataPartion(fireData)))

#
def task4DataPartion(table):
    result = []
    #asumme that we have 3 workers
    for i in range(3):
        result.append([])
    #for the balance work of each processor, I choose round roll bin partion
    for index, eachrecord in enumerate(table):
        index_bin = index%3
        result[index_bin].append(eachrecord)
    
    return result

def innerGroupBy(table):
    dic = {}
    for eachRecord in table:
        key = eachRecord[-2]
        if key not in dic:
            dic[key] = 1
        else:
            dic[key] += 1
        
    return dic


def parallelMergeGroupBy(table):
    result = {}
    pool = Pool(processes=3)
    
    local_result_list = []
    for i in table:
        local_result = pool.apply_async(innerGroupBy, [i])
        output = local_result.get()
        local_result_list.append(output)
    pool.close()

    for dic in local_result_list:
        for eachDate in dic:
            if eachDate not in result:
                result[eachDate] = dic.get(eachDate)
            else:
                result[eachDate] += dic.get(eachDate)
    
    return result
        
#
parallelMergeGroupBy(task4DataPartion(fireData))
    
        
    
    


        
