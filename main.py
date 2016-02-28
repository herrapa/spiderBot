import argparse
import re
import urllib
from urllib.parse import urlparse
from urllib.parse import urljoin
from urllib.parse import urlsplit
from urllib.parse import urlunsplit
import urllib.request
from bs4 import BeautifulSoup
import sqlite3 as lite

parser = argparse.ArgumentParser(description='Scan ze webz')
parser.add_argument('URLs', metavar='U', type=str, nargs='+',
                   help='URLS to start off with')

#keywordregex = re.compile('<meta\sname=["\']keywords["\']\scontent=["\'](.*?)["\']\s/>')
#linkregex = re.compile('<a\s*href=[\'|"](.*?)[\'"].*?>')

def main():
    print ("Bot starting!")

    args = parser.parse_args()

    print (args.URLs)

    urls = []
    urls.extend(args.URLs)

    con = None
    cursor = None
    try:
        con = lite.connect('test.db')
        cursor=con.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS Data(Id INT PRIMARY KEY, Url TEXT UNIQUE ON CONFLICT IGNORE)")
        while len(urls) != 0:
            url = urls.pop()
            print ("Processing: " + url, end='\n')
            for u in parse_urls(url):
                # in mem or in database? depth first or??
                cursor.execute("SELECT count(*) FROM Data WHERE Url = ?", (u,))
                data=cursor.fetchone()[0]
                if data==0:
                    #print('There is no component named %s'%name)
                    urls.append(u)
                    cursor.execute('insert into Data values(NULL,?)', (u,))
            con.commit()

    except lite.Error as e:
        print ("Error %s:" % e.args[0])
        sys.exit(1)

    finally:
        if con:
            con.close()
    print ("No more URLs :(")

def parse_urls(url):
    try:
        with urllib.request.urlopen(url) as response:
            html = response.read()
            soup = BeautifulSoup(html)
            links = soup.find_all('a')
            new_urls = set([])
            for tag in links:
                link = tag.get('href',None)
                if link is not None:
                    new_url = ""
                    if link.startswith("http"):
                        new_url = link
                        #new_urls.add(link)
                        #print ("Simple link: ", link, end='\n')
                    elif link.startswith("/"):
                        new_url = urljoin(url, link)
                        #new_urls.add(combined)
                        #print ("Combined: ", new_url, end='\n')
                    else:
                        continue
                    #new_url = "http://hej.com"

                    new_url = clean_url(new_url)
                    #print (new_url)
                    new_urls.add(new_url)
            return list(new_urls)
    except Exception as e:
        print ("Something went bad!", e, end='\n')

    return []

def clean_url(url):
    if "?" in url:
        url = url[:url.find("?")]
    if "#" in url:
        url = url[:url.find("#")]
    return url

if __name__ == '__main__':
    main()
