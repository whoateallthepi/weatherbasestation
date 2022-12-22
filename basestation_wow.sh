
#!/bin/bash
# Runs the basestation upload to wow
VENV="/home/pi/basestation/venv/"
WDIR="/home/pi/basestation/basestation/"
echo "starting python environment"
source $VENV/bin/activate
python --version
which python
cd $WDIR
python basestation_wow.py --debug  --station 4 --update
