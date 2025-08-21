# Tools to Learn About IPTV Services

This repository contains a set of Python tools designed to assist users of IPTV services in gaining detailed information about their services. All the tools are written in Python and are intended to be simple and user-friendly, allowing for easy modification and learning.

## Scripts and Tools

For more information on each specific tool and script, follow the links below:

1. [iptv-xstream-download.py](./README.iptv-xstream-download.md): This script is used to download all relevant files from an xtream provider and save them for future archiving or further processing.
2. [find-iptv-channels-details.py](./README.find-iptv-channels-details.md): This script queries an xtream provider’s live channel list and searches for specific channels or categories. It then notes the number of EPG programs available, whether they have catch-up capabilities, the codec and resolution for each channel, and the frame rate.

## Install

To install the tools, follow these steps:

1. Ensure that Python is installed and accessible from the command line. You can test this by running the following command and ensuring that the version numebr is printed out:

```bash
python3 —version
```

2. Clone the repository using the following command:

```bash
git clone https://github.com/jayyysss6666/iptv-tools
cd iptv-tools
```

3. Create a virtual environment and activate it using the following commands:

```bash
python3 -m venv venv
source venv/bin/activate
```

4. Install the required dependencies using the following command:

```bash
pip install -r requirements.txt
```

## Potential Enhancements

Here are some potential enhancements that might be added in the future -

1. Develop a program that summarizes changes between two sets of xtream details. This could highlight new movies or series added, new or deleted channels, new groups, and so on.

2. Create a program that searches EPG files for specific channel names or descriptions.

3. Develop a program that adds or overrides EPG records from a secondary EPG file.

4. Create an EPG-specific file that contains only the channels available on the user’s subscription.

5. Create a custom m3u8 playlist based on a provider’s xtream.


## Credits

ChatGPT has been extensively used on the evelopemnt and testing of these programs.  it is a game changer !
