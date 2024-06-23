Steps to run streaming service -
1. Run paragraph_generator.py to generate the text that has to be streamed.
2. Open a terminal in the src folder, run harinris_homework3.py
3. Once the service is running, open another terminal in the same location and run "ncat -lk 9999 < paragraph.txt". This will send the contents of the text file on port 9999. The output will now be visible on the terminal.