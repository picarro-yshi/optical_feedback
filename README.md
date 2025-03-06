# optical_feedback
Combine optical data with sensor data 


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


Notes:
1. For every optical csv file, it will generate a h5 file containing optical+sensor data. The generated h5 file will have the same name as the optical file.
2. Key matching: keys in the input data were expanded to include all keys in a given template; if that key don't have data, the script will fill its content with 0.
3. Uses multi-processing to speed up the merge process (3x faster than for-loop).


