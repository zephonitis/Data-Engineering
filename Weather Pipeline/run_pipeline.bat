@echo off
cd /d "C:\Users\ashwi\OneDrive\Desktop\DE\Weather Pipeline"
echo === Pipeline Run: %date% %time% === >> pipeline_log.txt
python main.py >> pipeline_log.txt 2>&1
echo. >> pipeline_log.txt
