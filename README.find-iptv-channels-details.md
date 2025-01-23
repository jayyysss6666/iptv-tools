# find-iptv-channels-details.py

This script queries an xtream provider’s live channel list and searches for specific channels or categories. It then notes the number of EPG programs available, whether they have catch-up capabilities, the codec and resolution for each channel, and the frame rate.

The script caches each day the live streams and categories data into two files in order to decrease excessive server calls.  You can ignore the files and force an actuial server request each time by using the --nocache option  

You are able to search for a combination of channel names and categories and see if the channel has archive capabilities (value shown is > 0). For each found channel, you can specify then if want to get for each found channel wether there is EPG data for it, as well as query the codec , resoution and frame rate for the channel

There is also a parameter (--save FILENAME.CSV) to save the output into a csv file  

## Sample Output

```bash

find-iptv-channels-details - Running for server xxxxx.cdngold.me on 2025-01-05 17:21

ID        Name                                                         Category                                Archive EPG  Codec   Resolution     Frame     
========================================================================================================================================================
414142    CA EN: TSN 1                                                 CA| SPORTS EN                           0       166  h264    1920x1080      30        
414141    CA EN: TSN 2                                                 CA| SPORTS EN                           0       139  h264    1920x1080      60        
414140    CA EN: TSN 3                                                 CA| SPORTS EN                           0       158  h264    960x540        60        
414139    CA EN: TSN 4                                                 CA| SPORTS EN                           0       169  h264    1280x720       30        
414138    CA EN: TSN 5                                                 CA| SPORTS EN                           0       166  h264    960x540        60        
```

## Usage

```bash
python3 find-iptv-channels-details.py [-h] --server SERVER --user USER --pw PW [--nocache] [--channel CHANNEL] [--category CATEGORY] [--debug] [--epgcheck] [--check] [--save SAVE]

Xtream IPTV Downloader and Filter

optional arguments:
  -h, --help           show this help message and exit
  --server SERVER      The Xtream server to connect to.
  --user USER          The username to use.
  --pw PW              The password to use.
  --nocache            Force download and ignore cache.
  --channel CHANNEL    Filter by channel name.
  --category CATEGORY  Filter by category name.
  --debug              Enable debug mode.
  --epgcheck           Check if channels provide EPG data and count entries.
  --check              Check stream resolution and frame rate using ffprobe.
  --formatname         Formats the name to include 4K, FHD, HD, SD for 3280, 1080, 720, <720 respectively.
  --delay              Adds a 5s delay to prevent returning 456 err, resulting in no stream being tested.
  --save SAVE          Save the output to a CSV file. Provide the file name.
```

## Examples

1. Look for categories that match CA| SPORTS EN, and then look for channel there that match TSN and a space.  Then for all of these programs check if they have EPG data, and also check their resoultion, codec and frame rate
```bash
python3 find-iptv-channels-details.py --server xxxx.cdngold.me --user myusername --pw  secret --category "CA| SPORTS EN" --channel "TSN "  --check  --epgcheck
```

2. Look for channels that match ESPN across all live channels and save them into file espnchannels.csv. 
```bash
python3 find-iptv-channels-details.py --server xxxx.cdngold.me --user myusername --pw  secret  --channel "ESPN" --save "espnchannels.csv"
```