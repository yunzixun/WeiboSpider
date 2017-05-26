# coding:utf-8
from db.user import get_user_by_uid
from tasks.workers import app
from page_get import user as user_get
from db.seed_ids import get_seed_ids, get_seed_by_id, insert_seeds, set_seed_other_crawled


@app.task(ignore_result=True)
def crawl_follower_fans(uid):
    seed = get_seed_by_id(uid)
    if seed.other_crawled == 0:
        rs = user_get.get_fans_or_followers_ids(uid, 1)
        rs.extend(user_get.get_fans_or_followers_ids(uid, 2))
        datas = set(rs)
        # 重复数据跳过插入
        if datas:
            insert_seeds(datas)
        set_seed_other_crawled(uid)


@app.task(ignore_result=True)
def crawl_person_infos(uid):
    """
    根据用户id来爬取用户相关资料和用户的关注数和粉丝数（由于微博服务端限制，默认爬取前五页，企业号的关注和粉丝也不能查看）
    :param uid: 用户id
    :return: 
    """
    if not uid:
        return

    # 由于与别的任务共享数据表，所以需要先判断数据库是否有该用户信息，再进行抓取
    user = user_get.get_profile(uid)
    # 不抓取企业号
    if user.verify_type == 2:
        set_seed_other_crawled(uid)
        return

    app.send_task('tasks.user.crawl_follower_fans', args=(uid,), queue='fans_followers',
                  routing_key='for_fans_followers')


@app.task(ignore_result=True)
def crawl_person_profile_infos(uid):
    """
    根据用户id来爬取用户相关资料和用户的关注数和粉丝数（由于微博服务端限制，默认爬取前五页，企业号的关注和粉丝也不能查看）
    :param uid: 用户id
    :return:
    """
    if not uid:
        return
    user_get.get_profile(uid)


@app.task(ignore_result=True)
def excute_user_task():
    seeds = get_seed_ids()
    if seeds:
        for seed in seeds:
            app.send_task('tasks.user.crawl_person_infos', args=(seed.uid,), queue='user_crawler',
                          routing_key='for_user_info')


@app.task(ignore_result=True)
def excute_user_profile_task():
    from db.weibo_repost import get_repost_uid
    seeds = get_repost_uid()
    count = 0
    if seeds:
        for seed in seeds:
            if not get_user_by_uid(seed):
                app.send_task('tasks.user.crawl_person_profile_infos', args=(seed,), queue='user_profile_crawler',
                              routing_key='or_user_profile_info')
                count += 1
    from db.wb_data import get_wbdata_uid
    seeds = get_wbdata_uid()
    if seeds:
        for seed in seeds:
            if not get_user_by_uid(seed):
                app.send_task('tasks.user.crawl_person_profile_infos', args=(seed,), queue='user_profile_crawler',
                              routing_key='or_user_profile_info')
                count += 1
    from db.weibo_comment import get_comment_uid
    seeds = get_comment_uid()
    if seeds:
        for seed in seeds:
            if not get_user_by_uid(seed):
                app.send_task('tasks.user.crawl_person_profile_infos', args=(seed,), queue='user_profile_crawler',
                              routing_key='or_user_profile_info')
                count += 1
    print('总共有{}个用户数据需要爬取'.format(count))
