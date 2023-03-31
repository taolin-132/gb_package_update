# encoding= utf-8
import configparser as ConfigParser
import logging
import os
import shutil
import time
import xml.etree.ElementTree as ET
from logging.handlers import RotatingFileHandler

import ksycopg2
import pypinyin

log_file = time.strftime("%Y%m%d") + '.log'
log_formatter = logging.Formatter('%(levelname)s %(asctime)s <----> %(message)s')
log_handler = RotatingFileHandler(log_file, mode='a', maxBytes=5 * 1024 * 1024, backupCount=0, encoding=None,
                                  delay=False)
# 生成一个RotatingFileHandler对象，限制日志文件大小为5M
log_handler.setFormatter(log_formatter)  # 对象载入日志格式
log_handler.setLevel(logging.DEBUG)  # 对象载入级别
app_log = logging.getLogger('log')  # 初始化logging模块
app_log.setLevel(logging.DEBUG)  # 设置初始化模块级别
app_log.addHandler(log_handler)  # 载入RotatingFileHandler对象


class Config:
    config = ConfigParser.ConfigParser()
    config.read("./config", encoding='utf-8')

    db_name = config.get('database', 'dbname')
    db_host = config.get('database', 'dbhost')
    db_port = config.get('database', 'dbport')
    db_user = config.get('database', 'dbuser')
    db_password = config.get('database', 'dbpassword')

    data_package = config.get('path_config', 'data_package')
    old_filepath = config.get('path_config', 'old_filepath')
    new_filepath = config.get('path_config', 'new_filepath')

    depart_id = config.get('other', 'depart_ID')


class kingbase:
    def __init__(self):
        self.database = Config.db_name
        self.user = Config.db_user
        self.password = Config.db_password
        self.host = Config.db_host
        self.port = Config.db_port

    def execu(self, sql):
        try:
            conn = ksycopg2.connect(
                "dbname={} user={} password={} host={} port={}".format(
                    self.database, self.user, self.password, self.host, self.port
                ))
        except Exception as e:
            print("数据库连接初始化错误:")
            print(e)
        else:
            print("数据库连接初始化成功")
        cur = conn.cursor()
        if str.upper(sql[0:6]) == "INSERT":
            sql_type = '插入'
        elif str.upper(sql[0:6]) == "SELECT":
            sql_type = '查询'
        elif str.upper(sql[0:6]) == "DELETE":
            sql_type = '删除'
        elif str.upper(sql[0:6]) == "UPDATE":
            sql_type = '更新'
        if sql_type == "查询":
            try:
                cur.execute(sql)
                rows = cur.fetchall()
                for row in rows:
                    for cell in row:
                        # print(type(cell))
                        # if type(cell) == unicode:
                        #     print(cell)
                        # else:
                        #     # print cell
                        return cell
            except Exception as e:
                print(e)
                print(sql_type + "失败")
            finally:
                cur.close()
                conn.commit()
                conn.close()
        else:
            try:
                cur.execute(sql)
            except Exception as e:
                print(sql + ":" + sql_type + "错误：")
                print(e)
            else:
                print(sql + "执行" + sql_type + "完成!")
            finally:
                cur.close()
                conn.commit()
                conn.close()


def jiexi_xml(list_file_allpath, highdefinitionfile_folder, originfile_folder):
    # 判断参数1是否为xml文件
    def pinyin(name):
        s = ''
        for a in name:
            s += pypinyin.lazy_pinyin(a, style=pypinyin.FIRST_LETTER)[-1]
        if s != '' or s is not None:
            return str.upper(str(s))
        else:
            return ''

    for xmlname in os.listdir(list_file_allpath):
        print(os.path.join(list_file_allpath, xmlname))
        if os.path.join(list_file_allpath, xmlname).endswith(".xml"):
            try:
                kingbases = kingbase()
                print("开始解析--, {}".format(os.path.join(list_file_allpath, xmlname)))
                app_log.info("开始解析--{}".format(os.path.join(list_file_allpath, xmlname)))
                # 通过 ElementTree以二进制方式读取xml文件
                tree = ET.parse(open(os.path.join(list_file_allpath, xmlname), 'rb'))
                # 获取xml文件的根
                root = tree.getroot()
                zzmm_code = ''
                daylwt = ''
                dazlr = ''
                dashr = ''
                dajs = ''
                dabsrq = ''
                szdacjr = ''
                szdashr = ''
                dabsdw = ''
                gzdwjzw = ''
                new_main = False
                # 遍历xml根下的标签
                for child in root:
                    # print(child.tag)
                    # 判断标签内容
                    if child.tag == '人员基本信息':
                        # 遍历标签内容下的标签
                        for i in child:
                            # 判断标签内容
                            if i.tag == '姓名':
                                # 判断 值是否为空或者None
                                if i.text is not None:
                                    # 将标签的值赋值给变量
                                    name = i.text
                                    # 通过pypinyin获取姓名的首拼
                                    if pinyin(name) != '' or pinyin(name) is not None:
                                        name_py = pinyin(name)
                                    else:
                                        name_py = ''
                                else:
                                    print(list_file_allpath.split("\\"[-1]) + '姓名不允许为空，此数据包未解析')
                                    app_log.error(list_file_allpath + '姓名不允许为空，此数据包未解析')
                                    break
                            elif i.tag == '性别':
                                if i.text is not None:
                                    if i.text == '男' or i.text == '1':
                                        xb = '1'
                                    elif i.text == '女' or i.text == '2':
                                        xb = '2'
                                    else:
                                        app_log.error("{}:性别错误".format(name))
                                        print('{}:性别错误'.format(name))
                                else:
                                    xb = ''
                            elif i.tag == '民族':
                                # isdigit判断是否是数字，如果是数字就查询数据库的档案系统的order_code，返回民族代码，
                                # 有“其他族”需要添加一个转换，因为加工系统的其他族和档案系统其他族代码不一致。
                                if i.text is not None:
                                    # 判断属性的值是否为数字，并且值不为57
                                    if i.text.isdigit() and i.text != '57':
                                        # 通过数据库查询到民族的码值
                                        mz_code = kingbases.execu(
                                            "SELECT CODE FROM SYS_CODE WHERE DICT_TYPE_ID ='13' AND ORDER_CODE = '{}'".format(
                                                i.text))
                                    elif i.text == '57':
                                        # “其他族”的码值
                                        mz_code = 'MZ_QTZ'
                                    else:
                                        mz = i.text
                                        try:
                                            # 将查询到的民族码值赋值给mz_code
                                            mz_code = kingbases.execu(
                                                "SELECT CODE FROM SYS_CODE WHERE DICT_TYPE_ID ='13' AND NAME = '{}'".format(
                                                    mz))
                                        except UnboundLocalError as e:
                                            # 如果查询有报错，则民族不存在
                                            print("{}的民族信息插入错误，原因是: {},".format(name, e))
                                            app_log.info("{}的民族信息插入错误，原因是: {},".format(name, e))
                                            mz_code = ''
                                else:
                                    # 若以上条件都不符合，则民族为空
                                    if mz_code == '':
                                        print("{},民族为空".format(list_file_allpath))
                                        app_log.info("{},民族为空".format(list_file_allpath))
                            elif i.tag == '出生日期':
                                # 判断出生日期长度，获取出生日期的格式
                                if len(i.text) == 7:
                                    # 将出生日期中的‘.’替换成‘-’
                                    csrq = i.text.replace('.', '-')
                                elif len(i.text) == 8 or len(i.text) == 6:
                                    cd = len(i.text)
                                    print(cd)
                                    # 使用数组的方式存储，遍历成一个个数字的出生日期
                                    array = []
                                    for csrq_list in i.text:
                                        array.append(csrq_list)
                                    # 拼接数字，组成 年，月的格式
                                    # print("{}-{}".format(''.join(array[0:4]), ''.join(array[4:6])))
                                    csrq = "{}-{}".format(''.join(array[0:4]), ''.join(array[4:6]))

                                else:
                                    print('{}出生日期存在问题'.format(name))
                                    app_log.error('{}出生日期存在问题'.format(name))
                            elif i.tag == '公民身份号码':
                                if i.text is not None:
                                    sfz = i.text
                                else:
                                    sfz = ''
                                    app_log.warning("{}:身份证空".format(name))
                                    print("{}:身份证空".format(name))

                            if sfz == '' or sfz is None:
                                staff_id == kingbases.execu(
                                    "SELECT DISTINCT REC_ID FROM STAFF_MAIN INNER JOIN STAFF_TIME_PROP ON STAFF_MAIN.REC_ID=STAFF_TIME_PROP.STAFF_ID AND PROP_TYPE='ZB01_CSRQ'  WHERE STAFF_NAME='{}' AND SEX='{}' AND STAFF_TIME_PROP.PROP_VAL=TO_DATE('{}','YYYYMMDD')".format(
                                        name, xb, csrq))
                            elif sfz != '' or sfz is not None:
                                try:
                                    staff_id == kingbases.execu(
                                        "SELECT DISTINCT REC_ID FROM STAFF_MAIN WHERE CERTIFICATE_CODE='{}' OR A0184='{}'".format(
                                            sfz, sfz))
                                except Exception as e:
                                    print("检测人员是否存在出现异常：{}".format(e))
                                if staff_id == '' or staff_id is None:
                                    # 从数据获取STAFF_MAIN序列的最新值
                                    staff_id = kingbases.execu("SELECT STAFF_MAIN_REC_ID_SEQ.NEXTVAL")
                                    app_log.info(
                                        "人员 {} 的 id是{}".format(list_file_allpath.split("\\")[-1], str(staff_id)))
                                    print("人员 {} 的 id是{}".format(list_file_allpath.split("\\")[-1], str(staff_id)))
                                    app_log.info(
                                        "人员 {} 的 id是{}".format(list_file_allpath.split("\\")[-1], str(staff_id)))

                        # 出生年月
                        try:
                            kingbases.execu(
                                "INSERT INTO STAFF_TIME_PROP(REC_ID,STAFF_ID,PROP_TYPE,PROP_VAL,UPDATE_TIME,UPDATE_ID,STAFF_TIME_STATUS) VALUES(STAFF_TIME_PROP_REC_ID_SEQ.NEXTVAL,{},'ZB01_CSRQ',to_date('{}','yyyymmdd'),now(),'100','0')".format(
                                    staff_id, csrq))
                        except Exception as e:
                            print('{}出生日期插入异常,原因是：{}'.format(name, e))
                            app_log.error('{}出生日期插入异常,原因是：{}'.format(name, e))
                        else:
                            app_log.info("{}出生年月插入完成".format(name))
                            print("{}出生年月插入完成".format(name))

                        # 人员基本信息
                        try:
                            kingbases.execu(
                                "INSERT INTO STAFF_MAIN(REC_ID,DEPART_ID,STAFF_NAME,SORT_NUM,STAFF_POLITICAL,STAFF_NATION,LEGACY_SHOW,ARCHIVE_FINISH,ARCHIVE_AUDIT,ARCHIVE_VOLUME,SUBMIT_TIME,NUM_ARCHIVES_COLLECTION,NUM_ARCHIVES_AUDIT,ARCHIVES_SUBMIT_UNIT,STAFF_NAME_PY,CREATOR,CREATED_TIME,SEX,CERTIFICATE_CODE,WORKING_STATE,COMPANY_NAME,BIRTHDAY,A0101,A0104,A0117,A0107,A0184,AUDIT_STATUS)VALUES ({},{},'{}','1','{}','{}','{}','{}','{}','{}',to_date('{}','yyyymm'),'{}','{}','{}','{}','100',now(),'{}','{}', 'RUZT_ZZ','{}','{}','{}','{}','{}','{}','{}','0')".format(
                                    staff_id, Config.depart_id, name, zzmm_code, mz_code, daylwt, dazlr, dashr, dajs,
                                    dabsrq, szdacjr, szdashr, dabsdw, name_py, xb, sfz, gzdwjzw, csrq, name, xb,
                                    mz_code, csrq,
                                    sfz))
                        except Exception as e:
                            print("{}基本信息插入失败,原因是：{}".format(name, e))
                            app_log.error("{}基本信息插入失败,原因是：{}".format(name, e))
                        else:
                            app_log.info("{}:人员基本信息插入完成".format(name))
                            print("{}:人员基本信息插入完成".format(name))

                    elif child.tag == '目录信息':
                        for i in child:
                            originfiles = []
                            highdefinitionfiles = []
                            # 计算第几张图片
                            sort_num = 0
                            for a in i:
                                if a.tag == '类号':
                                    leihao = a.text
                                    try:
                                        leihao_code = kingbases.execu(
                                            "SELECT  CODE FROM SYS_CODE WHERE DICT_TYPE_ID='37' AND SPLIT_PART(NAME,'、',1) = '{}'".format(
                                                leihao))
                                    except Exception as e:
                                        print("获取{}的{}出现异常，原因是：{}".format(name, leihao, e))
                                        app_log.error("获取{}的{}出现异常，原因是：{}".format(name, leihao, e))
                                    else:
                                        app_log.info("{}:类号，码值获取成功:{}".format(a.text, leihao_code))
                                        print("{}:类号，码值获取成功:{}".format(a.text, leihao_code))

                                elif a.tag == '序号':
                                    xuhao = a.text

                                elif a.tag == '材料名称':
                                    cailiao_name = a.text

                                elif a.tag == '材料形成时间':
                                    cailiao_time = a.text
                                    if len(cailiao_time) == 8:
                                        c = []
                                        for b in cailiao_time:
                                            c.append(b)
                                        cailiao_time_yyyy = ''.join(c[0:4])
                                        cailiao_time_MM = ''.join(c[4:6])
                                        cailiao_time_DD = ''.join(c[6:8])
                                    else:
                                        app_log.warning("长度应该为8位，但{}的{}材料形成时间只有" + str(
                                            len("{}").format(list_file_allpath, cailiao_name, cailiao_time)) + "位")
                                        print("长度应该为8位，但{}的{}材料形成时间只有" + str(len("{}")) + "位".format(
                                            list_file_allpath.split("\\")[-1], cailiao_name, cailiao_time))

                                elif a.tag == '页数':
                                    count_num = a.text

                                elif a.tag == '备注':
                                    if a.text is not None:
                                        beizhu = a.text
                                    else:
                                        beizhu = ''
                                    try:
                                        staff_arch_cata_rec_id = kingbases.execu(
                                            "SELECT staff_arch_cata_rec_id_SEQ.NEXTVAL")
                                    except Exception as e:
                                        print("{}:{}目录ID获取失败，原因是:{}".format(name, cailiao_name, e))
                                        app_log.error("{}:{}目录ID获取失败，原因是:{}".format(name, cailiao_name, e))
                                    else:
                                        app_log.info(
                                            "{}:{}目录ID获取成功:{}".format(name, cailiao_name, staff_arch_cata_rec_id))
                                        print(
                                            "{}:{}目录ID获取成功{}".format(name, cailiao_name, staff_arch_cata_rec_id))
                                    try:
                                        kingbases.execu(
                                            "INSERT INTO STAFF_ARCH_CATA(REC_ID,STAFF_ID,MATERIAL_TYPE,MATERIAL_NAME,MATERIAL_YYYY,MATERIRAL_MM,MATERIRAL_DD,MATERIRAL_YMD,MATERIAL_PAGE_NUM,REMARK,CREATOR_ID,CREATED_DATE,CATA_NUM,RSDAML000,RSDAML001,RSDAML002,RSDAML003,RSDAML004,RSDAML006,RSDAML007,RSDAML008,RSDAML005,RSDAML009) VALUES({}, {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '100',TO_CHAR(NOW(),'YYYY-mm-dd'), '{}', '{}',{}, '{}', '{}','{}', '{}', '{}','{}','{}','{}')".format(
                                                staff_arch_cata_rec_id, staff_id, leihao_code, cailiao_name,
                                                cailiao_time_yyyy, cailiao_time_MM, cailiao_time_DD, cailiao_time,
                                                count_num, beizhu, xuhao, staff_arch_cata_rec_id, staff_id, leihao_code,
                                                xuhao, cailiao_name, cailiao_time_yyyy, cailiao_time_MM,
                                                cailiao_time_DD, count_num, beizhu))
                                    except Exception as e:
                                        print("插入{}的{}目录异常,原因是：{}".format(name, cailiao_name, e))
                                        app_log.error("插入{}的{}目录异常,原因是：{}".format(name, cailiao_name, e))
                                    else:
                                        print("{}:{}目录数据插入成功".format(name, cailiao_name))
                                        app_log.info("{}:{}目录数据插入成功".format(name, cailiao_name))

                                elif a.tag == '原始图像数据_Text':
                                    originfile = a.text
                                    originfiles.append(originfile)
                                    sort_num = sort_num + 1

                                elif a.tag == '优化图像数据_Text':
                                    file1 = ''
                                    if leihao.count('-'):
                                        file1 = "0" + "{}{}".format(leihao.split('-')[0], leihao.split('-')[1])
                                    else:
                                        if len(leihao_code) == 1:
                                            file1 = str('0') + "{}".format(leihao_code)
                                        elif len(leihao_code) == 2:
                                            file1 = str(leihao_code)
                                    if len(xuhao) == 1:
                                        file2 = str('0') + "{}".format(xuhao)
                                    else:
                                        file2 = str(xuhao)
                                    if len(str(sort_num)) == 1:
                                        file3 = str('0') + str("{}").format(sort_num)
                                    elif len(str(sort_num)) == 2:
                                        file3 = str(sort_num)
                                    filename = "{}{}{}.JPG".format(file1, file2, file3)
                                    old_path = os.path.join(list_file_allpath, '图像数据', '原始图像数据', a.text)
                                    origin_move_path = os.path.join(originfile_folder, str(staff_id),
                                                                    str(staff_arch_cata_rec_id))
                                    try:
                                        copy_Photofile(old_path, origin_move_path, filename)
                                    except Exception as e:
                                        print("复制{}的{}原图出现异常，原因是{}".format(name, old_path, e))
                                        app_log.error("复制{}的{}原图出现异常，原因是{}".format(name, old_path, e))
                                    else:
                                        print("{}的{}原图复制成功".format(name, old_path))
                                        app_log.info("{}的{}原图复制成功".format(name, old_path))
                                    new_path = os.path.join(list_file_allpath, '图像数据', '优化图像数据', a.text)
                                    highDefinition_move_path = os.path.join(highdefinitionfile_folder, str(staff_id),
                                                                            str(staff_arch_cata_rec_id))
                                    try:
                                        copy_Photofile(new_path, highDefinition_move_path, filename)
                                    except Exception as e:
                                        print("复制{}的{}高清图出现异常，原因是{}".format(name, new_path, e))
                                        app_log.error("复制{}的{}高清图出现异常，原因是{}".format(name, new_path, e))
                                    else:
                                        print("{}的{}高清图复制成功".format(name, new_path))
                                        app_log.info("{}的{}高清图复制成功".format(name, new_path))
                                    highdefinitionfile = filename
                                    highdefinitionfiles.append(highdefinitionfile)
                                    pic_path = os.path.join(str(staff_id), str(staff_arch_cata_rec_id),
                                                            filename).replace('\\', '/')
                                    try:
                                        kingbases.execu(
                                            "INSERT INTO STAFF_ARCH_CATA_PIC(REC_ID, STAFF_ID, CATA_ID, SORT_NUM, PIC_PATH, ORI_PIC_PATH, CREATOR_ID, CREATED_DATE)VALUES(STAFF_ARCH_CATA_PIC_REC_ID_SEQ.NEXTVAL, "
                                            "{}, {}, '{}','{}', '{}' ,100, TO_CHAR(NOW(),'YYYY-mm-dd'));".format(
                                                staff_id, staff_arch_cata_rec_id, sort_num, pic_path, pic_path))
                                    except Exception as e:
                                        print("{}:档案图片插入异常，原因是：{}".format(name, e))
                                        app_log.error("{}:档案图片插入异常，原因是：{}".format(name, e))
                                    else:
                                        print("{}:{}:图片路径插入成功".format(name, filename))
                                        app_log.info("{}:{}:图片路径插入成功".format(name, filename))
            except Exception as e:
                print("{}解析异常，原因是：{}".format(os.path.join(list_file_allpath, xmlname), e))
                app_log.error("{}解析异常，原因是：{}".format(os.path.join(list_file_allpath, xmlname), e))
            else:
                print("解析 {}完成".format(list_file_allpath))
                app_log.info("解析 {}完成".format(list_file_allpath))
                return staff_id


def copy_Photofile(filename_path, movepath, filename):
    # print(filename, 'filname')
    if os.path.exists(movepath) == 0:
        # print("{}不存在".format(movepath))
        os.makedirs(movepath)
    # app_log.info("创建{}成功".format(movepath))
    # print("创建{}成功".format(movepath))
    try:
        shutil.copy(filename_path, os.path.join(movepath, filename))
    except Exception as e:
        print("复制异常，原因是：e".format(e))
        app_log.error("复制异常，原因是：e".format(e))
    else:
        print("文件{}已复制到:{}".format(filename_path, os.path.join(movepath, filename)))
        app_log.info("文件{}已复制到:{}".format(filename_path, os.path.join(movepath, filename)))


if __name__ == '__main__':
    if os.path.exists(Config.data_package):
        for dataname in os.listdir(Config.data_package):
            # os.path.isdir需要绝对路径
            if os.path.isdir(os.path.join(Config.data_package, dataname)):
                print(os.path.join(Config.data_package, dataname))
                jiexi_xml(os.path.join(Config.data_package, dataname), Config.new_filepath, Config.old_filepath)
    else:
        print("请检查配置文件中的data_package路径：{}是否存在".format(Config.data_package))
        app_log.error("请检查配置文件中的data_package路径：{}是否存在".format(Config.data_package))
