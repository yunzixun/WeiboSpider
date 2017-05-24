import sys

from sqlalchemy import desc

sys.path.append('/'.join(sys.path[0].split('/')[:-1]))

from db.basic_db import db_session
from db.wb_django.weibo2.models import WeiboData, KeyWords, KeywordsWbdata, User, WeiboRepost
import xlwt
from datetime import datetime

keyindex = {'序号': 0,
            '事件名': 1,
            '事件类型': 2,
            '微博名称': 3,
            '特征': 4,
            '微博属性': 5,
            '粉丝拥有量': 6,
            '网址': 7,
            '发布时间': 8,
            '转发数': 9,
            '情感值': 10,
            '第一层转发': 11,
            '第二层转发': 12,
            '第三层转发': 13,
            '第四层转发': 14,
            '四层以上转发': 15,
            '普通用户数量': 16,
            '个人认证占比': 17,
            '机构认证占比': 18,
            '一层占比': 19, }

for i in range(1, 11):
    count = 7
    keyindex['昵称{}'.format(i)] = 19 + (i - 1) * count + i
    keyindex['粉丝数{}'.format(i)] = 20 + (i - 1) * count + i
    keyindex['认证类型{}'.format(i)] = 21 + (i - 1) * count + i
    keyindex['微博数{}'.format(i)] = 22 + (i - 1) * count + i
    keyindex['等级{}'.format(i)] = 23 + (i - 1) * count + i
    keyindex['转发数{}'.format(i)] = 24 + (i - 1) * count + i
    keyindex['转发时间{}'.format(i)] = 25 + (i - 1) * count + i


def build_init_sheet(ws):
    for k, v in keyindex.items():
        ws.write(0, v, k)
    return ws


def get_repost_user_count(wbid, verify_type):
    return db_session.query(WeiboRepost).join(User, User.uid == WeiboRepost.user_id).filter(
        WeiboRepost.root_weibo_id == wbid).filter(
        User.verify_type == verify_type).count()


def get_repost_lv_count(wbid, lv):
    db_session.query(WeiboRepost).filter(WeiboRepost.root_weibo_id == wbid).filter(
        WeiboRepost.lv == lv).count()


def time_difference(last, before):
    last = datetime.strptime(last, '%Y-%m-%d %H:%M')
    before = datetime.strptime(before, '%Y-%m-%d %H:%M')
    diff = (last - before).seconds
    res = str(diff % 60) + '秒'
    diff //= 60
    if diff > 0:
        res = str(diff % 60) + '分' + res
    diff //= 60
    if diff > 0:
        res = str(diff % 24) + '小时' + res
    diff //= 24
    if diff > 0:
        res = str(diff) + '天' + res
    return res


def get_repost_count_by_user(wbid, user_id):
    db_session.query(WeiboRepost).filter(WeiboRepost.root_weibo_id == wbid).filter(
        WeiboRepost.user_id == user_id).order_by(desc(WeiboRepost.repost_count))


line_num = 0
finish_count = 0


def main():
    workbook = xlwt.Workbook()
    ws = build_init_sheet(workbook.add_sheet('统计'))
    keywords = db_session.query(KeyWords)# .filter(KeyWords.keyword == '曼彻斯特爆炸')
    for keyword in keywords:
        for wbid in db_session.query(KeywordsWbdata.wb_id).filter(KeywordsWbdata.keyword_id == keyword.id):
            for wb in db_session.query(WeiboData).filter(WeiboData.weibo_id == wbid[0]):
                build_one(keyword, wb, ws)
    workbook.save('result.xls')


def build_one(keyword, wb, ws):
    user = db_session.query(User).filter(User.uid == wb.uid).one()
    lv2 = db_session.query(WeiboRepost).filter(WeiboRepost.root_weibo_id == wb.weibo_id).filter(
        WeiboRepost.lv == 1).count()
    lv3 = db_session.query(WeiboRepost).filter(WeiboRepost.root_weibo_id == wb.weibo_id).filter(
        WeiboRepost.lv == 2).count()
    lv4 = db_session.query(WeiboRepost).filter(WeiboRepost.root_weibo_id == wb.weibo_id).filter(
        WeiboRepost.lv == 3).count()
    lv5 = db_session.query(WeiboRepost).filter(WeiboRepost.root_weibo_id == wb.weibo_id).filter(
        WeiboRepost.lv > 3).count()
    lv1 = db_session.query(WeiboRepost).filter(WeiboRepost.root_weibo_id == wb.weibo_id).filter(
        WeiboRepost.lv == 0).count()
    all_repost = lv1 + lv2 + lv3 + lv4 + lv5
    if all_repost < 100:
        return all_repost
        # continue
    global line_num
    line_num += 1
    print('{}行开始统计'.format(line_num))
    ws.write(line_num, keyindex['序号'], line_num)
    ws.write(line_num, keyindex['事件名'], keyword.keyword)
    ws.write(line_num, keyindex['微博名称'], user.name)
    ws.write(line_num, keyindex['粉丝拥有量'], user.fans_num)
    ws.write(line_num, keyindex['网址'], wb.weibo_url)
    ws.write(line_num, keyindex['发布时间'], wb.create_time)
    ws.write(line_num, keyindex['微博属性'], user.verify_type)
    # 转转发统计
    ws.write(line_num, keyindex['第一层转发'], lv1)
    ws.write(line_num, keyindex['第二层转发'], lv2)
    ws.write(line_num, keyindex['第三层转发'], lv3)
    ws.write(line_num, keyindex['第四层转发'], lv4)
    ws.write(line_num, keyindex['四层以上转发'], lv5)
    ws.write(line_num, keyindex['转发数'], all_repost)
    ws.write(line_num, keyindex['普通用户数量'], get_repost_user_count(wb.weibo_id, 0))
    ws.write(line_num, keyindex['个人认证占比'], get_repost_user_count(wb.weibo_id, 1))
    ws.write(line_num, keyindex['机构认证占比'], get_repost_user_count(wb.weibo_id, 2))
    ws.write(line_num, keyindex['一层占比'], (int((10000 * lv1 / (all_repost))) / 100) if all_repost > 0 else 0)
    i = 1
    for keyrepost in db_session.query(WeiboRepost).filter(
                    WeiboRepost.root_weibo_id == wb.weibo_id).order_by(
        desc(WeiboRepost.repost_count))[:10]:
        repost_user = db_session.query(User).filter(User.uid == keyrepost.user_id).one()
        ws.write(line_num, keyindex['昵称{}'.format(i)], repost_user.name)
        ws.write(line_num, keyindex['粉丝数{}'.format(i)], repost_user.fans_num)
        ws.write(line_num, keyindex['认证类型{}'.format(i)], repost_user.verify_type)
        ws.write(line_num, keyindex['微博数{}'.format(i)], repost_user.wb_num)
        ws.write(line_num, keyindex['等级{}'.format(i)], repost_user.level)
        ws.write(line_num, keyindex['转发数{}'.format(i)], keyrepost.repost_count)
        ws.write(line_num, keyindex['转发时间{}'.format(i)], time_difference(keyrepost.repost_time, wb.create_time))

        i += 1
    global finish_count
    finish_count += 1
    print('{}行完成'.format(finish_count))
    return all_repost


if __name__ == '__main__':
    main()




    # 事件名为keyword，
