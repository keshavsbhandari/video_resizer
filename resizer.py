from pathlib import Path
import json
import subprocess
from multiprocessing import Pool,Value,Manager
import progressbar as pb
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--source", help="source video folder eg: /home/keshav/data/kinetics/train", type=str, default='/home/keshav/data/kinetics/train',action="store")
parser.add_argument("--destination", help="destination folder eg: /keshav/data/kinetics_resized/train", type=str,default='kinetics_resized/train/',action="store")
parser.add_argument("--outsize", help="output size of video heightxwidth, input format eg: 256x256", type=str,default='256x256',action="store")
parser.add_argument("--outrate", help="output frame rate", type=str,default='10',action="store")
parser.add_argument("--nprocess", help="number of process", type=int,default=2,action="store")
args = parser.parse_args()

source = args.source
destination = args.destination
outsize = args.outsize
outrate = args.outrate
nbprocess = args.nbprocess


path = Path(source)
donepath = Path(destination)

donepath.mkdir(parents=True,exist_ok=True)

get_dest = lambda x: Path(destination,x.parent.name, x.name).as_posix().replace('.avi','.mp4')
get_source = lambda x:Path(source,x.parent.name,x.name.replace('.mp4','.avi')).as_posix()

get_dir_name = lambda x:Path(x[1]).parent

all_video = path.glob('*/**/*.avi')
done_video = donepath.glob('*/**/*.mp4')

done_pair = [*map(lambda x:(get_source(x),x.as_posix()),done_video)]

all_pair = [*map(lambda x:(x.as_posix(),get_dest(x)),all_video)]

remainng = list(set(all_pair).symmetric_difference(set(done_pair)))


counter = Value('i',0)
errorlist = Manager().list()

if not remainng:exit(0)

# remainng = remainng[slice(0,10000,200)]

dir_list = set(map(get_dir_name, remainng))
total_size = len(remainng)
wid = [pb.Percentage(),' | ',
       pb.SimpleProgress(),' ',
       pb.Bar(marker="#",left="[",right="]"),' ',
       pb.AnimatedMarker(),
       pb.ETA(),' | ',
       pb.AdaptiveTransferSpeed(unit='Video'),' | ',
       pb.AdaptiveETA(),' | ',
       pb.AbsoluteETA(), ' | ',
       pb.Timer()]
bar = pb.ProgressBar(widgets=wid, maxval=total_size).start()


def process(paths):
    """
    :param paths: list of pairs of source, destination video path
    :type paths: list of lists(of size 2, str path)
    """
    global done_list
    in_path, out_path = paths
    try:
        out = subprocess.check_call(['ffmpeg', '-y','-hwaccel', 'cuvid', '-c:v', 'h264_cuvid',
                     '-resize', outsize,'-y','-i', in_path,
                     '-c:a', 'copy', '-c:v', 'h264_nvenc','-b:v', '5M','-r', outrate,'-an',out_path],
                                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        counter.value += 1
        bar.update(counter.value)
    except subprocess.CalledProcessError as e:
        errorlist.append(paths)
        print(f'{out_path} cannot be written out of memory')
        if Path(out_path).exists():Path(out_path).unlink()
    except OSError as e:
        if Path(out_path).exists():Path(out_path).unlink()


def makedir(x):
    x.mkdir(exist_ok = True)

def filter(allpath,donepath):
    return list(set(allpath).symmetric_difference(set(donepath)))

def create_dir(n = 12):
    with Pool(processes=n) as pool:
        pool.map(makedir, dir_list, len(dir_list)//n)

def main(n = 2):
    create_dir()
    with Pool(processes=n) as pool:
        pool.map(process, remainng, len(remainng)//n)
    print("\nCOMPLETED")

if __name__ == '__main__':
    main(nbprocess)
    print(errorlist)
    json.dump(list(errorlist),open('error.txt','w'))

"""
source ~/anaconda3/bin/activate && conda activate attention && cd ~/data/resizer/ && python resizer.py --destination /home/keshav/data/kinetics_resized
"""
