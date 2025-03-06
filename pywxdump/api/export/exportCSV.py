import csv
import json
import os
from pywxdump.db import DBHandler
import time

def export_csv(wxid, outpath, db_config, my_wxid="我", page_size=5000, days=None):
    if not os.path.exists(outpath):
        outpath = os.path.join(os.getcwd(), "export" + os.sep + wxid)
        if not os.path.exists(outpath):
            os.makedirs(outpath)

    db = DBHandler(db_config, my_wxid)

    count = db.get_msgs_count(wxid)
    chatCount = count.get(wxid, 0)
    if chatCount == 0:
        return False, "没有聊天记录"

    if page_size > chatCount:
        page_size = chatCount + 1

    # 若传递了 days 参数，将其转换为整数并计算对应时间戳
    if days is not None:
        try:
            days = int(days)
            time_limit = time.time() - days * 24 * 60 * 60
        except ValueError:
            print(f"错误：传入的 days 参数 {days} 无法转换为整数。")
            return False, "时间筛选参数无效"
    else:
        time_limit = None

    users = {}
    for i in range(0, chatCount, page_size):
        start_index = i
        data, users_t = db.get_msgs(wxid, start_index, page_size)
        print(users, users_t)
        users.update(users_t)

        # 若设置了时间限制，筛选出符合条件的记录
        if time_limit is not None:
            from datetime import datetime
            # 将 CreateTime 转换为时间戳进行比较
            recent_data = [row for row in data if 
                           isinstance(row.get("CreateTime", 0), (int, float)) and float(row.get("CreateTime", 0)) >= time_limit or
                           isinstance(row.get("CreateTime", ""), str) and datetime.strptime(row.get("CreateTime", ""), "%Y-%m-%d %H:%M:%S").timestamp() >= time_limit]
        else:
            recent_data = data

        if len(recent_data) == 0:
            continue

        save_path = os.path.join(outpath, f"{wxid}_{i}_{i + page_size}.csv")

        with open(save_path, "w", encoding="utf-8", newline='') as f:
            csv_writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)

            csv_writer.writerow(["id", "MsgSvrID", "type_name", "is_sender", "talker", "room_name", "msg", "src",
                                 "CreateTime"])
            for row in recent_data:
                id = row.get("id", "")
                MsgSvrID = row.get("MsgSvrID", "")
                type_name = row.get("type_name", "")
                is_sender = row.get("is_sender", "")
                talker = row.get("talker", "")
                room_name = row.get("room_name", "")
                msg = row.get("msg", "")
                src = row.get("src", "")
                CreateTime = row.get("CreateTime", "")
                csv_writer.writerow([id, MsgSvrID, type_name, is_sender, talker, room_name, msg, src, CreateTime])

    with open(os.path.join(outpath, "users.json"), "w", encoding="utf-8") as f:
        json.dump(users, f, ensure_ascii=False, indent=4)

    return True, f"导出成功: {outpath}"


if __name__ == '__main__':
    pass
