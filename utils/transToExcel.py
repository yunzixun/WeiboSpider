import sys

sys.path.append('/'.join(sys.path[0].split('/')[:-1]))

from db.basic_db import db_session
from db.models import WeiboData, KeyWords, KeywordsWbdata, User, WeiboRepost
import xlwt

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
            '关键传播帐号1属性': 19,
            '时间1': 20,
            '关键传播帐号1转发量': 21,
            '关键传播帐号2属性': 22,
            '时间2': 23,
            '关键传播帐号2转发量': 24,
            '关键传播帐号3属性': 25,
            '时间3': 26,
            '关键传播帐号3转发量': 27,
            '关键传播帐号4属性': 28,
            '关键传播帐号4时间': 29,
            '关键传播帐号4转发量': 30,
            '关键传播帐号5属性': 31,
            '关键传播帐号5时间': 32,
            '关键传播帐号5转发量': 33,
            '关键传播帐号6属性': 34,
            '关键传播帐号6时间': 35,
            '关键传播帐号6转发量': 36,
            '关键传播帐号7属性': 37,
            '关键传播帐号7时间': 38,
            '关键传播帐号7转发量': 39,
            '关键传播帐号8属性': 40,
            '关键传播帐号8时间': 41,
            '关键传播帐号8转发量': 42,
            '关键传播帐号9属性': 43,
            '关键传播帐号9时间': 44,
            '关键传播帐号9转发量': 45,
            '关键传播帐号10属性': 46,
            '关键传播帐号10时间': 47,
            '关键传播帐号10转发量': 48,
            '一层占比': 49, }


def build_init_sheet(ws):
    for k, v in keyindex.items():
        ws.write(0, v, k)
    return ws


if __name__ == '__main__':
    workbook = xlwt.Workbook()
    ws = build_init_sheet(workbook.add_sheet('统计'))
    j = 1
    keywords = db_session.query(KeyWords).filter(KeyWords.keyword == '曼彻斯特爆炸').all()
    for keyword in keywords:
        for wbid in db_session.query(KeywordsWbdata.wb_id).filter(KeywordsWbdata.keyword_id == keyword.id).all():
            for wb in db_session.query(WeiboData).filter(WeiboData.weibo_id == wbid[0]).all():
                user = db_session.query(User).filter(User.uid == wb.uid).one()
                ws.write(j, keyindex['序号'], j)
                ws.write(j, keyindex['事件名'], keyword.keyword)
                ws.write(j, keyindex['微博名称'], user.name)
                ws.write(j, keyindex['粉丝拥有量'], user.fans_num)
                ws.write(j, keyindex['网址'], wb.weibo_url)
                ws.write(j, keyindex['发布时间'], wb.create_time)
                ws.write(j, keyindex['转发数'], wb.repost_num)

                # 转转发统计
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
                ws.write(j, keyindex['第一层转发'], lv1)
                ws.write(j, keyindex['第二层转发'], lv2)
                ws.write(j, keyindex['第三层转发'], lv3)
                ws.write(j, keyindex['第四层转发'], lv4)
                ws.write(j, keyindex['四层以上转发'], lv5)
                print(j)
                j += 1
    workbook.save('result.xls')



    # 事件名为keyword，
