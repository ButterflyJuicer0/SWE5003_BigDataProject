# from wss import Room
#
#
# # 填写抖音直播的链接
# room = Room("https://live.douyin.com/77217772750")
# room.connect()

import sys
from wss import Room

def main():
    # 检查命令行参数是否提供了直播链接
    if len(sys.argv) != 2:
        print("Usage: python main.py <live_url>")
        sys.exit(1)

    live_url = sys.argv[1]

    # 创建 Room 对象并连接
    room = Room(live_url)
    room.connect()

if __name__ == "__main__":
    main()
