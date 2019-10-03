from pathlib import Path
import json
import subprocess
from multiprocessing import Pool,Value,Manager
import progressbar as pb
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--source", help="increase output verbosity", type=str, default='/home/keshav/data/kinetics/train',action="store")
parser.add_argument("--destination", help="increase output verbosity", type=str,default='kinetics_resized/train/',action="store")
args = parser.parse_args()



"""
CREATING DIRECTORIES
s = lambda x: Path(x.as_posix().replace('/kinetics/', '/kineticsresize/')).mkdir(exist_ok = True)
path = Path('/home/keshav/DATA/kinetics/train')
dirpath = path.glob('*')
[*map(s,dirpath)]
"""

source = args.source
destination = args.destination


path = Path(source)
donepath = Path(destination)

donepath.mkdir(parents=True,exist_ok=True)
# get_dest = lambda x: x.as_posix().replace('/kinetics/', '/kineticsresize/')
chunks = lambda l, n: [l[x: x+n] for x in range(0, len(l), n)]

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
    :param paths:
    :type paths:
    :return:
    :rtype:
    """
    global done_list
    in_path, out_path = paths
    try:
        out = subprocess.check_call(['ffmpeg', '-y','-hwaccel', 'cuvid', '-c:v', 'h264_cuvid',
                     '-resize', '256x256','-y','-i', in_path,
                     '-c:a', 'copy', '-c:v', 'h264_nvenc','-b:v', '5M','-r', '10','-an',out_path],
                                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        counter.value += 1
        bar.update(counter.value)

        # print(f'{counter.value} of {total_size} {100 * counter.value/50:.2f}% DONE')
    except subprocess.CalledProcessError as e:
        errorlist.append(paths)
        print(f'{out_path} cannot be written out of memory')
        if Path(out_path).exists():Path(out_path).unlink()
    except OSError as e:
        if Path(out_path).exists():Path(out_path).unlink()
    # '-t', '00:00:10.0'


def makedir(x):
    x.mkdir(exist_ok = True)

def filter(allpath,donepath):
    return list(set(allpath).symmetric_difference(set(donepath)))

def create_dir(n = 12):
    with Pool(processes=n) as pool:
        pool.map(makedir, dir_list, len(dir_list)//n)

def main(n = 1):
    create_dir()
    with Pool(processes=n) as pool:
        pool.map(process, remainng, len(remainng)//n)
    print("\nCOMPLETED")

if __name__ == '__main__':
    main()
    print(errorlist)
    json.dump(list(errorlist),open('error.txt','w'))

    # for d,_ in list(errorlist):
    #     Path(d).unlink()


"""
source ~/anaconda3/bin/activate && conda activate attention && cd ~/data/resizer/ && python resizer.py --destination /home/keshav/data/kinetics_resized
"""
