### Installing Requirements
#!pip3 install -r requirements.txt

import os
import yaml

def main(args):

    # Setting variables for script
    JOBS_API_URL = args[1] #"https://z4xgdztf.cde-6fr6l74r.go01-dem.ylcu-atmi.cloudera.site/dex/api/v1"
    WORKLOAD_USER = args[2] #"cdpusername"

    ### CDE CLI Setup
    os.system("cd ~")
    os.system("mkdir .cde")

    ### Recreating Yaml file with your credentials:
    dict_yaml = {"user" : WORKLOAD_USER,
                 "vcluster-endpoint": JOBS_API_URL}

    with open(r'.cde/config.yaml', 'w') as file:
      documents = yaml.dump(dict_yaml, file)

    ### Manually upload the CDE CLI before running the below commands:

    os.system("mv ~/Documents/cde_first_steps/cli/cde ~/usr/local/bin")
    os.system("chmod 777 ~/usr/local/bin/cde")

    ### Do not run these
    #!export PATH=/home/cdsw/.cde:$PATH
    #!wget $CDE_CLI_linux -P /home/cdsw/.local/bin

if __name__ == '__main__':
    main(sys.argv)
