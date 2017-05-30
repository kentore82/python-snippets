import logging
import traceback

## Init logger
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
my_logger = logging.getLogger("mylogger")
my_hdlr = logging.FileHandler('./logs/my.log') #write to log file
my_hdlr.setFormatter(formatter)
my_logger.addHandler(my_hdlr) 
my_logger.setLevel(logging.INFO) #Set log level INFO, WARN, ERROR

#Catch exeptions
try:
    1/0
    my_logger.info('This worked fine!')
except Exception as e:
    my_logger.error('Oh shit!: '.format(traceback.format_exc()))
    exit(1)
