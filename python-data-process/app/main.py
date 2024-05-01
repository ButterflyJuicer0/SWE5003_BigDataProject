import threading
import gift_data_process
import member_data_process
import like_data_process
import social_data_process
from chat_data_process import chat_data_process

if __name__ == "__main__":
    # 创建每个进程的实例
    process_for_gift = gift_data_process.GiftDataProcess()
    process_for_member = member_data_process.MemberDataProcess()
    process_for_like = like_data_process.LikeDataProcess()
    process_for_social = social_data_process.SocialDataProcess()

    # 创建线程列表
    threads = []

    thread_chat = threading.Thread(target=chat_data_process)
    threads.append(thread_chat)
    thread_chat.start()

    thread_gift = threading.Thread(target=process_for_gift.process)
    threads.append(thread_gift)
    thread_gift.start()

    thread_member = threading.Thread(target=process_for_member.process)
    threads.append(thread_member)
    thread_member.start()

    thread_like = threading.Thread(target=process_for_like.process)
    threads.append(thread_like)
    thread_like.start()

    thread_social = threading.Thread(target=process_for_social.process)
    threads.append(thread_social)
    thread_social.start()

    # 等待所有线程完成
    for thread in threads:
        thread.join()
