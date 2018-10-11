#Import glob for handeling filename patterns
import glob
logs = glob.glob('*.log*')
for log in logs:
    with open(log) as readLog, open('parsedIPs.txt','a') as writeLog:
        #Skip the 1st line as it is a type of header in the logs
        next(readLog)
        for line in readLog:
            parsed = line.split()
            writeLog.write (parsed[-1])
            writeLog.write ("\n")
    writeLog.close()
readLog.close()
