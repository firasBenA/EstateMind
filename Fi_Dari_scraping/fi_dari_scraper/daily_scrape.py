# daily_scrape.py — Cycle quotidien Fi-Dari
import os, re, time, json, random, requests, psycopg2, unicodedata
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

DB_CONFIG={"host":"localhost","database":"estate_mind_db","user":"postgres","password":"admin"}
MEDIA_ROOT=r"C:\EstateMind\media_fidari"
BASE_URL="https://fi-dari.tn"
B="[[37.649,7.778],[30.107,11.953]]"
MAX_PAGES_DAILY=3

CATEGORIES={
    f"search?objectif=vendre&categorie=Appartement&usage=Habitation&bounds={B}":("sale","apartment"),
    f"search?objectif=louer&categorie=Appartement&usage=Tout+type+de+location&bounds={B}":("rent","apartment"),
    f"search?objectif=vendre&categorie=Maison&usage=Habitation&bounds={B}":("sale","house"),
    f"search?objectif=louer&categorie=Maison&usage=Tout+type+de+location&bounds={B}":("rent","house"),
    f"search?objectif=vendre&categorie=Terrain&bounds={B}":("sale","land"),
    f"search?objectif=louer&categorie=Terrain&bounds={B}":("rent","land"),
    f"search?objectif=vendre&categorie=Bureau&usage=Professionnels&bounds={B}":("sale","office"),
    f"search?objectif=louer&categorie=Bureau&usage=Bureaux+et+commerce&bounds={B}":("rent","office"),
}

PROPERTY_CONFIG={
    'apartment':{'has_rooms':True,'has_surface':True},
    'house':{'has_rooms':True,'has_surface':True},
    'land':{'has_rooms':False,'has_surface':True},
    'office':{'has_rooms':False,'has_surface':True}
}

GOUVERNORAT_ZONE={
    'Tunis':'grand_tunis','Ariana':'grand_tunis','Ben Arous':'grand_tunis','Manouba':'grand_tunis',
    'Bizerte':'nord','Nabeul':'nord','Beja':'nord','Jendouba':'nord','Zaghouan':'nord',
    'Sousse':'sahel','Monastir':'sahel','Mahdia':'sahel',
    'Kairouan':'centre','Kasserine':'centre','Sidi Bouzid':'centre','Siliana':'centre','Gafsa':'centre',
    'Sfax':'sud','Gabes':'sud','Medenine':'sud','Tataouine':'sud','Tozeur':'sud','Kebili':'sud'
}

FEATURE_MAPPING={
    'Climatisation':'has_air_conditioning','Ascenseur':'has_elevator','Jardin':'has_garden',
    'Piscine':'has_pool','Terrasse':'has_terrace','Parking':'has_parking',
    'Vue sur mer':'has_sea_view','Meuble':'is_furnished','Balcon':'has_balcony',
    'Digicode':'has_digicode','Lumineux':'is_bright','Calme':'is_quiet'
}

_PARASITE_KW=[
    'biatimmo','fleximmo','logo','icon','/icons/','picto','sprite','favicon','avatar',
    'placeholder','loading','noimage','no-image','banner','ads/','agence','promoteur',
    'facebook','instagram','youtube','google','gstatic','map-marker','check','arrow','btn-',
    '.svg','.gif','.ico'
]
_PHOTO_EXT={'.jpg','.jpeg','.png','.webp'}

def _norm(s):
    return unicodedata.normalize('NFKD',s.lower()).encode('ascii','ignore').decode()

CITY_GEO={_norm(k):v for k,v in {
    'tunis':('grand_tunis','Tunis'),'ariana':('grand_tunis','Ariana'),
    'ben arous':('grand_tunis','Ben Arous'),'manouba':('grand_tunis','Manouba'),
    'la marsa':('grand_tunis','Tunis'),'marsa':('grand_tunis','Tunis'),
    'bardo':('grand_tunis','Tunis'),'el menzah':('grand_tunis','Ariana'),
    'ennasr':('grand_tunis','Ariana'),'soukra':('grand_tunis','Ariana'),
    'nabeul':('nord','Nabeul'),'hammamet':('nord','Nabeul'),'bizerte':('nord','Bizerte'),
    'jendouba':('nord','Jendouba'),'beja':('nord','Beja'),'zaghouan':('nord','Zaghouan'),
    'kelibia':('nord','Nabeul'),'korba':('nord','Nabeul'),
    'sousse':('sahel','Sousse'),'monastir':('sahel','Monastir'),'mahdia':('sahel','Mahdia'),
    'msaken':('sahel','Sousse'),'kantaoui':('sahel','Sousse'),
    'sfax':('sud','Sfax'),'gabes':('sud','Gabes'),'jerba':('sud','Medenine'),
    'djerba':('sud','Medenine'),'medenine':('sud','Medenine'),'tataouine':('sud','Tataouine'),
    'tozeur':('sud','Tozeur'),'kebili':('sud','Kebili'),'zarzis':('sud','Medenine'),
    'gafsa':('centre','Gafsa'),'kairouan':('centre','Kairouan'),'kasserine':('centre','Kasserine'),
    'sidi bouzid':('centre','Sidi Bouzid'),'siliana':('centre','Siliana')
}.items()}

def _is_parasite(url):
    u=url.lower()
    ext=os.path.splitext(u.split('?')[0])[1]
    if ext in ('.svg','.gif','.ico','.bmp'):
        return True
    return any(kw in u for kw in _PARASITE_KW)

def _is_photo(url):
    u=url.lower().split('?')[0]
    return any(u.endswith(e) for e in _PHOTO_EXT)

def _find_city(text):
    if not text:
        return None
    t=_norm(text)
    for city in sorted(CITY_GEO,key=len,reverse=True):
        if re.search(r'\b'+re.escape(city)+r'\b',t):
            zone,region=CITY_GEO[city]
            return zone,region,city.title()
    return None

def build_location(raw,title='',desc=''):
    for txt in [raw,title,desc[:300]]:
        r=_find_city(txt)
        if r:
            zone,region,city=r
            mun=city
            break
    else:
        region='Tunisie'; mun='Tunisie'; zone='autre'
    parts=[p.strip() for p in (raw or '').replace(' - ',',').split(',') if p.strip()]
    if parts and _find_city(parts[0]):
        mun=parts[0].title()
    return {
        'region':region,'municipality':mun,'zone':zone,
        'location_details':{'region':region,'municipality':mun,'zone':zone,'raw':raw}
    }

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def url_exists(cur,u):
    cur.execute('SELECT 1 FROM fi_dari_listings WHERE url=%s',(u,))
    return cur.fetchone() is not None

def extract_rooms(t):
    if not t:
        return None
    t=t.lower()
    for pat in [r'\bs\s*\+?\s*(\d)\b',r'\b[ft]\s*(\d)\b',r'(\d)\s*pi[ee]ces?',r'\bstudio\b']:
        m=re.search(pat,t)
        if m:
            if 'studio' in pat:
                return 1
            v=int(m.group(1))
            return v if 1<=v<=15 else None
    return None

def extract_surface(t):
    if not t:
        return None
    m=re.search(r'([\d][\d\s.,]*)\s*m\s*[2]',t.lower())
    if m:
        try:
            v=float(m.group(1).replace(' ','').replace(',','.'))
            return v if 10<=v<=50000 else None
        except:
            pass
    return None

def extract_price(t):
    if not t:
        return None
    t=re.sub(r'(?i)\b(tnd|dt|dinar)\b','',str(t))
    t=t.replace('\xa0','').replace('\u202f','')
    d=re.sub(r'[^\d.,]','',t.strip())
    if not d:
        return None
    if re.match(r'^\d{1,3}[.,]\d{3}$',d):
        d=d.replace('.','').replace(',','')
    elif d.count('.')>1:
        d=d.replace('.','')
    d=d.replace(',','.')
    try:
        v=float(d)
        return v if 500<=v<=100_000_000 else None
    except:
        return None

def download_images(urls,ptype,lid):
    folder=os.path.join(MEDIA_ROOT,ptype,str(lid))
    os.makedirs(folder,exist_ok=True)
    if not urls:
        return None,folder,0
    saved=[]
    hdrs={'User-Agent':'Mozilla/5.0'}
    try:
        from PIL import Image as _PI
        import io as _io
        _pil=True
    except:
        _pil=False
    for url in dict.fromkeys(u for u in urls if u and not u.startswith('data:')):
        if _is_parasite(url) or not _is_photo(url):
            continue
        uhd=re.sub(r'[_-](thumb|small|medium)','',url,flags=re.I)
        for u in [uhd,url]:
            try:
                r=requests.get(u,headers=hdrs,timeout=12)
                if r.status_code==200 and len(r.content)>=40_000:
                    if _pil:
                        try:
                            img=_PI.open(_io.BytesIO(r.content))
                            if img.size[0]<300 or img.size[1]<200:
                                break
                        except:
                            pass
                    ct=r.headers.get('Content-Type','')
                    ext='webp' if 'webp' in ct else 'png' if 'png' in ct else 'jpg'
                    fp=os.path.join(folder,f'image_{len(saved)+1:03d}.{ext}')
                    with open(fp,'wb') as f:
                        f.write(r.content)
                    saved.append(fp)
                    time.sleep(0.2)
                    break
            except:
                continue
    return (saved[0] if saved else None),folder,len(saved)

def insert_listing(cur,d):
    try:
        cur.execute(
            'INSERT INTO fi_dari_listings(title,price,transaction_type,type,region,municipality,zone,location_details,surface,rooms,features,poi,description,url,pdf_link,image_path,images_folder,images_count,last_updated,is_new) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(url) DO NOTHING RETURNING id',
            (d.get('title'),d.get('price'),d.get('transaction_type'),d.get('type'),
             d.get('region'),d.get('municipality'),d.get('zone'),json.dumps(d.get('location_details',{})),
             d.get('surface'),d.get('rooms'),json.dumps(d.get('features',{})),json.dumps(d.get('poi',{})),
             d.get('description',''),d.get('url'),d.get('pdf_link'),None,None,0,d.get('last_updated','Unknown'),True))
        row=cur.fetchone()
        return row[0] if row else None
    except Exception as e:
        print(f'Insert err:{e}')
        return None

def update_images(cur,lid,t,f,c):
    cur.execute('UPDATE fi_dari_listings SET image_path=%s,images_folder=%s,images_count=%s WHERE id=%s',(t,f,c,lid))

def log_start(cur,rt):
    cur.execute('INSERT INTO fi_dari_log(run_type) VALUES(%s) RETURNING id',(rt,))
    return cur.fetchone()[0]

def log_end(cur,lid,n,e,s='done'):
    cur.execute('UPDATE fi_dari_log SET finished_at=NOW(),total_new=%s,total_errors=%s,status=%s WHERE id=%s',(n,e,s,lid))

def reset_is_new(cur):
    cur.execute('UPDATE fi_dari_listings SET is_new=FALSE')

def create_driver():
    opts=Options()
    opts.add_argument('--headless=new')
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    opts.add_argument('--window-size=1920,1080')
    opts.add_experimental_option('excludeSwitches',['enable-automation'])
    opts.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36')
    opts.set_capability('goog:loggingPrefs',{'performance':'ALL'})
    d=webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=opts)
    d.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
    return d

def deep_scroll(driver,wait=10):
    time.sleep(wait)
    total=driver.execute_script('return document.body.scrollHeight')
    step=max(200,total//15)
    pos=0
    while pos<total:
        driver.execute_script(f'window.scrollTo(0,{pos});')
        time.sleep(0.3)
        pos+=step
        nh=driver.execute_script('return document.body.scrollHeight')
        if nh>total:
            total=nh
    driver.execute_script('window.scrollTo(0,document.body.scrollHeight);')
    time.sleep(2)
    driver.execute_script('window.scrollTo(0,0);')
    time.sleep(1)

def get_annonces(driver,url):
    driver.get(url)
    deep_scroll(driver,10)
    soup=BeautifulSoup(driver.page_source,'html.parser')
    urls=set()
    for a in soup.find_all('a',href=True):
        h=a['href']
        if any(p in h for p in ['/bien/','/annonce/','/detail/']):
            if not h.startswith('http'):
                h=BASE_URL+h
            urls.add(h)
    return [{'url':u,'title_hint':'','price_hint':'','location_hint':'','thumb':None} for u in urls]

def scrape_detail(driver,url):
    try:
        driver.get(url)
        deep_scroll(driver,7)
        soup=BeautifulSoup(driver.page_source,'html.parser')
        data={}
        h1=soup.find('h1')
        data['title_detail']=h1.get_text(strip=True) if h1 else ''
        desc=''; best=0
        for tag in soup.find_all(['div','p','section']):
            cs=' '.join(tag.get('class',[]))
            if any(x in cs.lower() for x in ['description','detail','content','text','body']):
                t=tag.get_text(' ',strip=True)
                if 30<len(t)<5000 and len(t)>best:
                    best=len(t); desc=t
        data['description']=desc
        ptext=re.compile(r'([\d][\d\s.,]*)[\s]*(?:dt|tnd|dinar)',re.I)
        m=ptext.search(soup.get_text(' '))
        if m:
            data['price']=extract_price(m.group(1))
        for tag in soup.find_all('time'):
            data['last_updated']=tag.get('datetime','') or tag.get_text(strip=True)
            break
        data.setdefault('last_updated','Unknown')
        for a in soup.find_all('a',href=True):
            h=a['href']
            txt=a.get_text(strip=True).lower()
            if h.lower().endswith('.pdf') or any(k in txt for k in ['fiche','telecharger','pdf']):
                data['pdf_link']=h if h.startswith('http') else BASE_URL+'/'+h.lstrip('/')
                break
        data.setdefault('pdf_link',None)
        loc=''
        for nav in soup.find_all(['nav','ol'],class_=re.compile(r'breadcrumb|ariane',re.I)):
            parts=[i.get_text(strip=True) for i in nav.find_all(['li','a','span']) if i.get_text(strip=True)]
            geo=[p for p in parts[1:-1] if 2<len(p)<60]
            if geo:
                loc=' - '.join(geo)
                break
        if not loc:
            m2=re.search(r'/bien/([^/?#]+)',url)
            if m2:
                loc=m2.group(1).replace('-',' ')
        data['location_raw']=loc
        imgs=[]; seen=set()
        def add(src):
            if not src or len(src)<15:
                return
            if src.startswith('//'):
                src='https:'+src
            if not src.startswith('http'):
                src=BASE_URL+'/'+src.lstrip('/')
            if not _is_parasite(src) and _is_photo(src) and src not in seen:
                seen.add(src); imgs.append(src)
        for img in soup.find_all('img'):
            for attr in ['data-big','data-original','data-src','src']:
                src=img.get(attr,'')
                if src:
                    add(src)
                    break
        data['img_urls']=imgs
        data['surface_detail']=extract_surface(desc)
        data['rooms_detail']=extract_rooms(desc)
        result={}
        for li in soup.find_all('li'):
            t=li.get_text(strip=True)
            if 2<len(t)<80:
                for fr,en in FEATURE_MAPPING.items():
                    if fr.lower() in t.lower():
                        result[en]=True
                        break
        data['features']=result
        return data
    except Exception as e:
        print(f'detail err:{e}')
        return {}

def build_url(slug,page):
    base=f'{BASE_URL}/{slug}'
    base=re.sub(r'&page=\d+','',base)
    return f'{base}&page={page}'

def run_daily():
    now=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sep='='*60
    print(f'\n{sep}\n  CYCLE QUOTIDIEN Fi-Dari - {now}\n{sep}')
    os.makedirs(MEDIA_ROOT,exist_ok=True)
    conn=get_conn(); cur=conn.cursor(); driver=create_driver()
    log_id=log_start(cur,'daily'); reset_is_new(cur); conn.commit()
    total_new=0; total_errors=0
    try:
        for slug,(tt,pt) in CATEGORIES.items():
            cat_new=0
            print(f'\n  {pt} | {tt}')
            for page_num in range(1,MAX_PAGES_DAILY+1):
                url_page=build_url(slug,page_num)
                anns=[]
                try:
                    anns=get_annonces(driver,url_page)
                except:
                    driver=create_driver()
                if not anns:
                    break
                new_this=0
                for ann in anns:
                    ann_url=ann.get('url','')
                    if not ann_url or url_exists(cur,ann_url):
                        continue
                    print(f'    NEW {ann_url[-60:]}')
                    det={}
                    try:
                        det=scrape_detail(driver,ann_url)
                    except:
                        pass
                    price=det.get('price') or extract_price(ann.get('price_hint',''))
                    loc_raw=det.get('location_raw','') or ann.get('location_hint','')
                    desc=det.get('description','')
                    title=det.get('title_detail','') or 'N/A'
                    geo=build_location(loc_raw,title,desc)
                    cfg=PROPERTY_CONFIG.get(pt,{'has_rooms':True,'has_surface':True})
                    surface=(det.get('surface_detail') or extract_surface(desc)) if cfg['has_surface'] else None
                    rooms=(det.get('rooms_detail') or extract_rooms(desc)) if cfg['has_rooms'] else None
                    data={
                        'title':title,'price':price,'transaction_type':tt,'type':pt,
                        **{k:geo[k] for k in ['region','municipality','zone','location_details']},
                        'surface':surface,'rooms':rooms,'features':det.get('features',{}),'poi':{},
                        'description':desc,'url':ann_url,'pdf_link':det.get('pdf_link'),
                        'last_updated':det.get('last_updated','Unknown')
                    }
                    lid=insert_listing(cur,data)
                    if not lid:
                        total_errors+=1
                        continue
                    conn.commit()
                    imgs=list(det.get('img_urls',[]))
                    if ann.get('thumb') and not _is_parasite(ann['thumb']) and _is_photo(ann['thumb']):
                        if ann['thumb'] not in imgs:
                            imgs.insert(0,ann['thumb'])
                    t,f,n=download_images(imgs,pt,lid)
                    update_images(cur,lid,t,f,n)
                    conn.commit()
                    cat_new+=1; total_new+=1; new_this+=1
                    mun=geo['municipality']
                    rgn=geo['region']
                    print(f'    OK id={lid} {mun},{rgn} prix={price} imgs={n}')
                    time.sleep(random.uniform(2,4))
                if new_this==0:
                    break
                time.sleep(random.uniform(2,5))
            print(f'  {pt}_{tt} -> {cat_new} nouvelles')
    except Exception as e:
        print(f'ERREUR:{e}')
        log_end(cur,log_id,total_new,total_errors,'error')
        conn.commit()
    else:
        log_end(cur,log_id,total_new,total_errors,'done')
        conn.commit()
    finally:
        try:
            driver.quit()
        except:
            pass
        cur.close()
        conn.close()
    print(f'\n  TERMINE - {total_new} nouvelles annonces')

if __name__=='__main__':
    run_daily()
