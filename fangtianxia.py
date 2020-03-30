import re
import requests
import threading
from urllib import parse
from bs4 import BeautifulSoup
from multiprocessing import Process , Queue
from sqlalchemy import create_engine #创建引擎
from sqlalchemy import Column,String,Integer,Text #创建表
from sqlalchemy.orm import sessionmaker #利用sessionmaker来代替conn，执行数据库语句
from sqlalchemy.ext.declarative import declarative_base #导入基类
#创建引擎
Base = declarative_base()
engine = create_engine(
    "mysql+pymysql://root:123456@127.0.0.1:3306/test",
    max_overflow=5,
    pool_size=10,
    echo=True,
)
#创建表结构
class Host(Base):
    __tablename__ = "fangtianxia"
    id = Column(Integer,primary_key=True,autoincrement=True)
    title = Column(Text) #标题
    rent = Column(Text) #租金
    mode = Column(Text) #出租方式
    apartment = Column(Text) #户型
    area = Column(Text) #建筑面积
    orientations = Column(Text) #朝向
    floor = Column(Text) #楼层
    fit_up = Column(Text) #装修
    describe = Column(Text) #房源描述

# Base.metadata.creat_all(engine)
#创建实例
Session = sessionmaker(bind=engine)
sessi = Session()


sess = requests.session()
headers = {
    "cookie":"global_cookie=dfbgmq7rgz9naenx4g1yrth8n20k4xu00l3; integratecover=1; city=sh; keyWord_recenthousesh=%5b%7b%22name%22%3a%22%e6%b5%a6%e4%b8%9c%22%2c%22detailName%22%3a%22%22%2c%22url%22%3a%22%2fhouse-a025%2f%22%2c%22sort%22%3a1%7d%5d; __utmc=147393320; ASP.NET_SessionId=h4egryztjsulwztguaslumru; Rent_StatLog=27e5c6b5-89b5-4314-a1d9-e4cb51810dc7; __utma=147393320.1551951974.1578036186.1578453350.1578458404.15; __utmz=147393320.1578458404.15.14.utmcsr=search.fang.com|utmccn=(referral)|utmcmd=referral|utmcct=/captcha-2d479bc52f25c6bf44/redirect; g_sourcepage=zf_fy%5Elb_pc; __utmt_t0=1; __utmt_t1=1; __utmt_t2=1; Captcha=496C6B444D7436397A746548757239585365326551693378506355474877526E485541342F77496E58632B793563776B4F7134614645746F6D6779523156746862322F317A3152735542493D; __utmb=147393320.15.10.1578458404; unique_cookie=U_4nqtausgftw6adscfdhmm4lpk1sk54qd85j*8",
    "referer":"https://sh.zu.fang.com/",
    "user-agent":"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.70 Safari/537.36"
}

#获取详情页
def detail(url):

    print (url)
    #请求获得详情页
    req = sess.get(url,headers = headers)
    soup = BeautifulSoup(req.text,"lxml")
    #通过美丽汤获取标题
    try:
        title = soup.select(".title")[0]
        print ("标题：",title.text.strip())

    #获取租金
        rents = soup.select(".trl-item.sty1")[0]
        print (rents)
        compiles = re.compile(r'.*?<i>(\d+)</i>(.*?)（.*?<div>')
        rent = compiles.findall(rents.text)
        print ("租金：", rent)
    #获取出租方式
        mode = soup.select(".tt")[0]
        print ("出租方式：",mode.text.strip())
    #获取户型
        apartment = soup.selsect(".tt")[1]
        print ("户型：",apartment.text.strip())
    #获取建筑面积
        area = soup.selsect(".tt")[2]
        print ("建筑面积：",area.text.strip())
    #获取朝向
        orientations = soup.selsect(".tt")[3]
        print ("朝向：",orientations.text.strip())
    #获取楼层
        floor = soup.selsect(".tt")[4]
        print ("楼层：",floor.text.strip())
    #获取装修
        fit_up = soup.selsect(".tt")[5]
        print ("装修：",fit_up.text.strip())
    # 获取房源描述
        titles = soup.select(".fyms_title.floatl.gray9")
        t1 = []
        d1 = []
        for t in titles:
            t1.append(t.text.strip())
        detailed_information = soup.select(".fyms_con.floatl.gray3")
        for d in detailed_information:
            d1.append(d.text.strip())
        for i in range(len(t1)):
            # pass
            print (t1[i],d1[i])

        fangtianxia = Host(title = title, #标题
                            rent = rent, #租金
                            mode = mode, #出租方式
                            apartment = apartment, #户型
                            area = area, #建筑面积
                            orientations = orientations, #朝向
                            floor = floor, #楼层
                            fit_up = fit_up, #装修
                            describe = t1&d1 #房源描述)
        sessi.add(Host)
        sessi.commid()

    except Exception as e:
        print("详情页出错", e.__traceback__.tb_lineno)
        # print (req.text)
        # soup.title.string == "跳转..."
        # compiless = re.compile(r"(.*?)?")
        # urlss = compiless.findall(url)[0]
        # print (urlss)
        jumps2(req.text)
        # print (req.text)

        # pass

def jumps2(html):
    try:
        print (html)
        req = html
        rules1 = re.compile(r"var t1='(.*?)';")
        rules = re.compile(r"var t3='(.*?)';")
        url = rules1.findall(req)[0]
        urls = rules.findall(req)[0]
        jumps_url = url + "?" + urls
        # print (jumps_url)
        # t4 = threading.Thread(target=detail,args=[jumps_url,])
        # t4.start()
        # t4.join()
        detail(jumps_url)
    except Exception as e:
        print ("jumps2错误",e)
        pass

#获取翻页url
def jumps(url):
    try:
        req = sess.get(url,headers = headers)
        rules = re.compile(r"var t3='(.*?)';")
        urls = rules.findall(req.text)[0]
        jumps_url = url + "?" + urls
        # t4 = threading.Thread(target=detail,args=[jumps_url,])
        # t4.start()
        # t4.join()
        # detail(jumps_url)
    except:
        print ("jumps错误")
        pass

#获取详情页url
def details_url(url):
    try:
        # print (url)
        if "i31" in url:
            print("是")
            urls = re.compile(r"(.*?)i31/$")
            url = urls.findall(url)[0]
        #获取详情页url
        url = url
        req = sess.get(url,headers = headers)
        soup = BeautifulSoup(req.text,"lxml")
        detail = soup.select(".title a")
        for d in detail:
            details_urls = "https://sh.zu.fang.com" + d["href"]
            # print (details_urls)
            # t3 = threading.Thread(target=jumps,args=[details_urls,])
            # t3.start()
            # t3.join()
            jumps(details_urls)
    except:
        print ("details_url出错")
        pass

#获取区域url
def page_url(url):
    try:
        #获取区域页面
        req = sess.get(url,headers = headers)
        soup = BeautifulSoup(req.text,"lxml")
        # 获取每个区域的总页数
        pag = soup.select(".txt")[-1]
        page = pages(pag.text)
        # print (page)
        #构造每页的url
        p1 = []
        for p in range(1,page + 1):
            urls = parse.urljoin(url,"i3{}/".format(p))
            t2 = threading.Thread(target=details_url,args=[urls,])
            t2.start()
            t2.join()
            details_url(urls)
    except:
        print("page_url出错")
        pass


#每个区域页数
def pages(soup):
    try:
        #通过正则返回页数
        keyword = re.compile(r"共(.*?)页")
        s = keyword.findall(soup)[0]
        # print (s)
        return int(s)
    except:
        print ("pages出错")
        pass

#初始url
def region_url(url):
    #获取初始页面
    req = sess.get(url , headers = headers)
    soup = BeautifulSoup(req.text,"lxml")
    #获取地区的url
    r1 = []
    regions = soup.select("#rentid_D04_01 a")[1:]
    q = Queue()
    for r in regions:
        urls = parse.urljoin(url,r["href"])
        q.put(urls)
        print (urls)
    q2 = q.get()
    t1 = Process(target=page_url,args=[q2,])
    print (q2)
    t1.start()
    t1.join()
        # t1.join()
        # page_url(urls)
        # r1.append(urls)



if __name__ == '__main__':
    url = "https://sh.zu.fang.com/"
    region_url(url)
    print ("end")
