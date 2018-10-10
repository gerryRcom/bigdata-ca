import glob
logs = glob.glob("C:/Users/Administrator/OneDrive - gerryR.com/DBS-Content/Big-Data/CA/code/*.log")
for log in logs:
    with open(log) as readLog, open('C:/Users/Administrator/OneDrive - gerryR.com/DBS-Content/Big-Data/CA/code/parsedIPs.txt','a') as writeLog:
        next(readLog)
        for line in readLog:
            parsed = line.split()
            writeLog.write (parsed[-1])
            writeLog.write ("\n")
    writeLog.close()
readLog.close()