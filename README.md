# video_resizer
USAGE : resizer.py downsample framerate and resize videos

--source : video folder source
--destination : video folder destination
--outsize : output video size eg: for 256 by 256 pass 256x256
--outrate : output frame rate
--nbprocess : number of parallel process for multiprocessing

Requirements:
  python3.7, progressbar2, nvidia-toolkit cuda10.1, cuda enabled ffmpeg
  ffmpeg: https://developer.nvidia.com/ffmpeg
  
