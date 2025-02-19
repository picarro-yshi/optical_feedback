# optical_feedback
Combine optical data with sensor data 

For every optical csv file, it will generate a h5 file containing optical+sensor data.

- How to run
first update yaml
$ cd folder
$ python -m venv venv
$ source venv/bin/activate
$(venv) python -m pip install -r requirements.txt

- run this script to continuesly record sensor data, press ctrl+C to stop
$ cd optical_feedback
$ python stream.py

- run this script to merge optical and sensor data
$ python merge.py


