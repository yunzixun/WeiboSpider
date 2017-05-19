import sys
sys.path.append('/'.join(sys.path[0].split('/')[:-1]))

from db.basic_db import db_session
from db.models import WeiboData

if __name__ == '__main__':
    wb_datas = db_session.query(WeiboData).all()
