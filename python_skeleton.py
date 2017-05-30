from __future__ import print_function

import os,sys,getopt

def help():
    print(\
'''Usage:
    
    python python_skeleton.py 
            -i/--input1 <explain>
            -j/--input2 <explain>
''')


def main(argv):

    ######### Take care of input arguments
    if len(argv)==0:
        print("Not enough args!!")
        sys.exit()

    try:
        opts, args = getopt.getopt(argv,"i:j:h:",["input1","input2","help"])

    except getopt.GetoptError:
        help()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ('-h',"--help"):
            help()
            sys.exit()
        elif opt in ("-i", "--input1"):
       		#do stuff
			pass 
        elif opt in ("-j", "--input2"):
            #do stuff
			pass
        else:
			#do stuff
			sys.exit()
    ########### End take care of input args

	
    #### Start script
    print("World Hello!")


# Run main
if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
