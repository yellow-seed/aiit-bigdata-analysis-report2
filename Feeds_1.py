#
# python3 Feeds_1.py
#
import pandas as pd
import feedparser
import re
import requests
from bs4 import BeautifulSoup
from pdfminer.high_level import extract_text
import io

def get_webpage_content(url):
    # URLからデータ取得 (GET)
    print('URL # {}'.format(url), flush=True)
    try:
        res = requests.get(url)
        content_type = res.headers['Content-Type']
        if 'text/html' in content_type:
            soup = BeautifulSoup(res.content, 'html.parser')
            main_txt = soup.main
            # main_text == Noneならmainタグがない
            if main_txt == None:
                text = soup.get_text()
                text.replace('\n', '').replace('\u3000', ' ')
            else:
                text = soup.main.get_text()
                text.replace('\n', '').replace('\u3000', ' ')
        elif 'application/pdf':
            text2 = []
            ext_text = extract_text(io.BytesIO(res.content))
            
            # 行ごとに分割してから不要な改行コードを削除
            for l in ext_text.split('\n\n'):
                text2.append(l.replace('\n', ''))
            # その後もう一度結合し直す
            text = ' '.join(text2)
        return text
    except:
        return ''

def get_feeds(n, url):
    """
    urlから取得したフィードをリストにして返す。
    """
    # 初期化
    feeds = []
    # urlからフィードを取得
    try:
        f = feedparser.parse(url)
        if f.bozo != 0 and f.bozo != True:
            print('Error(bozo) url:', url, flush=True)
            return feeds
    except:
        print('Error(exception) url:', url, flush=True)
        return feeds
    # f.entries 内の各要素について処理
    # - title: タイトル
    # - summary, description: 内容
    for e in f.entries:
        # タイトル
        if 'title' in e:
            title = e.title
        else:
            title = ''

        # リンク(コンテンツ本体のURL)
        if 'link' in e:
            link = e.link
        else:
            link = ''

        # 内容：summary または description
        if 'summary' in e:
            body = e.summary
        elif 'description' in e:
            body = e.description
        else:
            body = ''
        
        # title と body の両方が空ならば追加しない
        if title == '' and body == '':
            continue

        # HTML 形式の場合があるため <...> を削除
        body = re.compile(r'<[^>]+>').sub('', body)
        # feeds に URL, タイトル、内容 を追加
        # - body.strip(): 先頭、末尾の改行・空白文字を削除

        content = get_webpage_content(link)

        # contentが空ならば追加しない
        if content == '':
            continue

        feeds.append([url, title, link, body.strip(), content])

    return feeds

def write_feeds(feedlist, output, simple_output):
    """
    feedlistに記載のURLからフィードを取得し、CSV形式でoutputファイルに書き出す。
    outputファイルが既にあれば、読み込み、重複排除を行う。
    """
    try:
        # outputファイル（CSV形式）から読み込み
        df = pd.read_csv(output)
        s_df = pd.read_csv(simple_output)
    except:
        # outputファイルがなかった場合、DataFrameを作成
        df = pd.DataFrame([], columns=['url', 'title', 'link', 'summary', 'content'])
        s_df = pd.DataFrame([], columns=['title', 'link'])

    # feedlistに記載のURLからフィードを取得
    urls = [line.strip() for line in open(feedlist)]
    for i, url in enumerate(urls):
        feeds = get_feeds(i, url)
        feeds_df = pd.DataFrame(feeds, columns=['url', 'title', 'link', 'summary', 'content'])
        df = pd.concat([df, feeds_df])
        s_df = pd.concat([s_df, feeds_df.loc[:, ['title', 'link']]])

    # 重複排除
    df = df.drop_duplicates()
    s_df = s_df.drop_duplicates()
    # CSV形式でoutputファイルに書き出し
    df.to_csv(output, index=False)
    s_df.to_csv(simple_output, index=False)

# フィードの取得、書き出し
# ハッカーニュースのRSSの場合、その時点での人気の記事が順番に表示されている
write_feeds('feedlist_en.txt', 'output_en.csv', 'index_output.csv')
# write_feeds('feedlist_jp.txt', 'output_jp.csv')