"""
Created on Aug 25, 2018
@author: suchat_s
"""
import os
import io
import shutil
import pyodbc
import logging


class ConnectDB:
    def __init__(self):
        # Test Add Comment
        self._connection = pyodbc.connect('Driver={SQL Server};Server=192.168.2.52;\
                                        Database=WebVendor_V2;uid=sa;pwd=P@ssw0rd')
        self._cursor = self._connection.cursor()

    def query(self, query):
        global result
        try:
            result = self._cursor.execute(query)
        except Exception as e:
            logging.error('error execting query "{}", error: {}'.format(query, e))
            return None
        finally:
            return result

    def update(self, sqlStatement):
        try:
            self._cursor.execute(sqlStatement)
        except Exception as e:
            logging.error('error execting Statement "{}", error: {}'.format(sqlStatement, e))
            return None
        finally:
            self._cursor.commit()

    def exec_sp(self, sqlStatement, params):
        try:
            self._cursor.execute(sqlStatement, params)
        except Exception as e:
            logging.error('error execting Statement "{}", error: {}'.format(sqlStatement, e))
            return None
        finally:
            self._cursor.commit()

    def __del__(self):
        self._cursor.close()


def getDefaultParamter():
    myConnDB = ConnectDB()
    sqlStr = r"""SELECT  PARAM_VLUE
                FROM    dbo.MST_Param
                WHERE   PARAM_CODE = 'WD_DOC_GR_PATH'
                ORDER BY PARAM_SEQN"""
    result_set = myConnDB.query(sqlStr).fetchall()
    # index value
    # 0 = Result Path -> dev2\webvndGR\Result
    # 1 = Backup Result path -> dev2\webvndGR\Result_Backup
    # 2 = Log path -> dev2\log
    # 3 = IP -> \\192.168.2.52\
    # print(result_set[3][0], result_set[2][0], result_set[1][0], result_set[0][0])
    src_path = str(result_set[3][0]) + str(result_set[0][0])
    des_path = str(result_set[3][0]) + str(result_set[1][0])
    log_path = str(result_set[3][0]) + str(result_set[2][0])

    # print(src_path, des_path, log_path)
    return src_path, des_path, log_path


def readDataInFile(fileFullPath):
    myConnDB = ConnectDB()

    fileObj = io.open(fileFullPath, 'r', encoding='utf-8')
    for linef in fileObj.readlines():
        # print(linef)
        val = linef.split(";")
        vs_RefID = val[0]
        vs_FileName = val[1]
        vs_FormatErrorFlag = val[2]
        vs_ErrorCode = val[3]
        vs_ErrorMsg = val[4]
        vs_GRErrorFlag = val[5]
        vs_GRYear = val[6]
        vs_GRNumber = val[7]
        vs_GRItem = val[8]
        vs_PONumber = val[9]
        vs_POItem = val[10]
        vs_MaterialItem = val[11]
        vs_UserName = val[12]
        vs_GRCreateDate = val[13]
        vs_GRCreateTime = val[14]

        #         print( vs_RefID +
        #                 vs_FileName +
        #                 vs_FormatErrorFlag +
        #                 vs_ErrorCode +
        #                 vs_ErrorMsg +
        #                 vs_GRErrorFlag +
        #                 vs_GRYear +
        #                 vs_GRNumber +
        #                 vs_GRItem +
        #                 vs_PONumber +
        #                 vs_POItem +
        #                 vs_MaterialItem +
        #                 vs_UserName +
        #                 vs_GRCreateDate +
        #                 vs_GRCreateTime )

        logging.debug('{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}'.format(vs_RefID, vs_FileName,
                                                                         vs_FormatErrorFlag, vs_ErrorCode, vs_ErrorMsg,
                                                                         vs_GRErrorFlag, vs_GRYear, vs_GRNumber,
                                                                         vs_GRItem,
                                                                         vs_PONumber, vs_POItem, vs_MaterialItem,
                                                                         vs_UserName, vs_GRCreateDate, vs_GRCreateTime))

        params = (vs_RefID, vs_FileName, vs_FormatErrorFlag, vs_ErrorCode, vs_ErrorMsg, vs_GRErrorFlag,
                  vs_GRYear, vs_GRNumber, vs_GRItem, vs_PONumber, vs_POItem, vs_MaterialItem, vs_UserName,
                  vs_GRCreateDate, vs_GRCreateTime)
        myConnDB.exec_sp("""
        EXECUTE [dbo].[sp_GRLogFromSAP] 
            @RefID = ?,
            @FileName = ?, 
            @FormatErrorFlag = ?,
            @ErrorCode = ?,
            @ErrorMsg = ?,
            @GRErrorFlag = ?,
            @GRYear = ?,
            @GRNumber = ?,
            @GRItem = ?,
            @PONumber = ?,
            @POItem = ?,
            @MaterialItem = ?,
            @UserName = ?,
            @GRCreateDate = ?,
            @GRCreateTime = ?
        """, params)


def archiveFiletoBKPath(fileFullPath, des_path):
    logging.info('Start Backup File to Destination Path [{}]'.format(fileFullPath))

    try:
        shutil.move(fileFullPath, des_path)
    except shutil.Error as err:
        logging.error('Error [{}]'.format(err))

    logging.info('End Backup File to Destination Path [{}]'.format(fileFullPath))


def main(src_path, des_path):
    for root, dirs, files in os.walk(src_path):
        for file in files:
            if file.endswith(".txt"):
                fileFullPath = src_path + "\\" + file
                logging.debug('Working in file [{}]'.format(fileFullPath))
                readDataInFile(fileFullPath)
                logging.debug('Finish file [{}]'.format(fileFullPath))
                archiveFiletoBKPath(fileFullPath, des_path)


if __name__ == '__main__':
    # Get Default Parameter from Master Parameter
    src_path, des_path, log_path = getDefaultParamter()
    # print(src_path, des_path, log_path)
    # src_path = r"D:\tmp\webvndGR\Result"
    # des_path = r"D:\tmp\webvndGR\Result_Backup"

    # src_path = r"\\192.168.2.52\dev2\webvndGR\Result"
    # des_path = r"\\192.168.2.52\dev2\webvndGR\Result_Backup"

    # log_path = r"D:\tmp\log"
    # log_path = r"\\192.168.2.52\dev2\log"
    logFile = log_path + '\ImportGRFromSAP.log'

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)-5s [%(levelname)-8s] >> %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=logFile,
                        filemode='a')

    logging.debug('#####################')
    logging.info('Start Process')
    main(src_path, des_path)
    logging.info('End Process')
    logging.debug('#####################')
