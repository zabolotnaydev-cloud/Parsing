# Парсер для выкачки нужных мне блогеров

!pip install google-api-python-client pandas --quiet
print('OK')

CONFIG = {
    'API_KEY': 'AIzaSyDvT8heR67IrlTS-k2iv1K8nxDgHjhbAg8',

    'GAME_TITLE': 'Gang Beasts',

    'MIN_SUBSCRIBERS': 3_000,
    'MAX_SUBSCRIBERS': 500_000,

    'N': 15,

    'MAX_SEARCH_PAGES': 10,

    'VIDEOS_PER_PAGE': 50,

    'OUTPUT_FILE': 'youtube_dataset_gang_beasts.csv',

    'CPM_BY_NICHE': {
        'micro':  2.5,   # 3k   - 10k
        'small':  3.0,   # 10k  - 50k
        'mid':    3.5,   # 50k  - 150k
        'large':  4.0,   # 150k - 500k
    },

    'SEARCH_QUERIES': [
        'Gang Beasts gameplay',
        'Gang Beasts review',
        'Gang Beasts funny moments',
        'Gang Beasts lets play',
        'Gang Beasts multiplayer',
        'Gang Beasts highlights',
        'Gang Beasts best moments',
        'Gang Beasts fails',
        'Gang Beasts online',
        'играем в Gang Beasts',
    ],
}

print('Конфигурация задана')
print(f'  Игра:             {CONFIG["GAME_TITLE"]}')
print(f'  Подписчики:       {CONFIG["MIN_SUBSCRIBERS"]:,} - {CONFIG["MAX_SUBSCRIBERS"]:,}')
print(f'  Поисковых запросов: {len(CONFIG["SEARCH_QUERIES"])}')
print(f'  Страниц на запрос:  {CONFIG["MAX_SEARCH_PAGES"]} x {CONFIG["VIDEOS_PER_PAGE"]} видео')
print(f'  Видео на канал:     {CONFIG["N"]}')
est_quota = len(CONFIG['SEARCH_QUERIES']) * CONFIG['MAX_SEARCH_PAGES'] * 100
print(f'  Расход квоты (оценка): ~{est_quota:,} из 10,000 ед./день')

import re
import math
import time
import logging
import pandas as pd
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

def search_all_channels(youtube, query, max_pages, per_page):
    found   = {}
    token   = None
    page    = 0

    while page < max_pages:
        try:
            params = dict(
                part='snippet',
                q=query,
                type='video',
                order='relevance',
                maxResults=per_page,
            )
            if token:
                params['pageToken'] = token

            resp  = youtube.search().list(**params).execute()
            items = resp.get('items', [])

            for item in items:
                cid   = item['snippet']['channelId']
                cname = item['snippet']['channelTitle']
                if cid not in found:
                    found[cid] = cname

            token = resp.get('nextPageToken')
            page += 1
            log.info(f'    Страница {page}: +{len(items)} видео, каналов всего: {len(found)}')

            if not token:
                log.info('    Страниц больше нет')
                break

            time.sleep(0.5)

        except HttpError as e:
            log.error(f'search.list error: {e}')
            break

    return found

def get_channels_info_batch(youtube, channel_ids):
    result = {}
    ids    = list(channel_ids)

    for i in range(0, len(ids), 50):
        batch = ids[i:i+50]
        try:
            resp = youtube.channels().list(
                part='statistics,snippet',
                id=','.join(batch)
            ).execute()
            for item in resp.get('items', []):
                cid  = item['id']
                subs = int(item['statistics'].get('subscriberCount', 0))
                name = item['snippet'].get('title', '')
                result[cid] = {'followers': subs, 'blogger_name': name}
        except HttpError as e:
            log.error(f'channels.list batch error: {e}')

    return result

def get_niche(followers):
    if followers < 10_000:
        return 'micro'
    elif followers < 50_000:
        return 'small'
    elif followers < 150_000:
        return 'mid'
    else:
        return 'large'

def get_video_ids(youtube, channel_id, n):
    video_ids = []
    try:
        resp = youtube.search().list(
            part='id',
            channelId=channel_id,
            maxResults=n,
            order='date',
            type='video'
        ).execute()
        for item in resp.get('items', []):
            vid = item.get('id', {}).get('videoId')
            if vid:
                video_ids.append(vid)
    except HttpError as e:
        log.error(f'search.list (videos) {channel_id}: {e}')
    return video_ids

def _iso8601_to_seconds(duration):
    m = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration or '')
    if not m:
        return 0
    h, mins, secs = (int(m.group(i) or 0) for i in (1, 2, 3))
    return h * 3600 + mins * 60 + secs

def get_videos_metrics(youtube, video_ids):
    metrics = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        try:
            resp = youtube.videos().list(
                part='statistics,contentDetails,snippet',
                id=','.join(batch)
            ).execute()
            for item in resp.get('items', []):
                stats   = item.get('statistics', {})
                details = item.get('contentDetails', {})
                snippet = item.get('snippet', {})
                metrics.append({
                    'viewCount':            int(stats.get('viewCount',    0)),
                    'likeCount':            int(stats.get('likeCount',    0)),
                    'commentCount':         int(stats.get('commentCount', 0)),
                    'duration_sec':         _iso8601_to_seconds(details.get('duration', 'PT0S')),
                    'liveBroadcastContent': snippet.get('liveBroadcastContent', 'none'),
                })
        except HttpError as e:
            log.error(f'videos.list error: {e}')
    return metrics

def aggregate(videos):
    if not videos:
        return None
    n            = len(videos)
    avg_views    = sum(v['viewCount']    for v in videos) / n
    avg_likes    = sum(v['likeCount']    for v in videos) / n
    avg_comments = sum(v['commentCount'] for v in videos) / n
    if avg_views == 0:
        return None
    engagement_rate = (avg_likes + avg_comments) / avg_views
    formats = []
    for v in videos:
        if v['duration_sec'] < 60:
            formats.append('shorts')
        elif v['liveBroadcastContent'] == 'live':
            formats.append('livestream')
        else:
            formats.append('long_video')
    return {
        'avg_views':       round(avg_views, 2),
        'engagement_rate': round(engagement_rate, 6),
        'content_format':  max(set(formats), key=formats.count),
    }

def calc_price(avg_views, cpm):
    return round(avg_views / 1000 * cpm, 2)

def calc_campaign_result(avg_views, er):
    return round(math.log(avg_views) * er, 6) if avg_views > 0 else 0.0

def collect_dataset(config):
    youtube      = build('youtube', 'v3', developerKey=config['API_KEY'])
    n            = config['N']
    min_subs     = config['MIN_SUBSCRIBERS']
    max_subs     = config['MAX_SUBSCRIBERS']
    game_title   = config['GAME_TITLE']
    rows         = []

    log.info('ФАЗА 1: Поиск каналов по запросам...')
    all_channels = {}

    for query in config['SEARCH_QUERIES']:
        log.info(f'  Запрос: "{query}"')
        found = search_all_channels(
            youtube, query,
            config['MAX_SEARCH_PAGES'],
            config['VIDEOS_PER_PAGE']
        )
        new = {k: v for k, v in found.items() if k not in all_channels}
        all_channels.update(new)
        log.info(f'  Новых каналов: {len(new)} | Всего уникальных: {len(all_channels)}')
        time.sleep(1)

    log.info(f'\nФАЗА 1 завершена. Уникальных каналов до фильтрации: {len(all_channels)}')

    log.info('\nФАЗА 2: Получение данных каналов (подписчики)...')
    channels_info = get_channels_info_batch(youtube, list(all_channels.keys()))

    filtered = {
        cid: info
        for cid, info in channels_info.items()
        if min_subs <= info['followers'] <= max_subs
    }
    log.info(f'  После фильтра {min_subs:,}-{max_subs:,} подписчиков: {len(filtered)} каналов')

    log.info('\nФАЗА 3: Сбор метрик каналов...')
    total = len(filtered)

    for idx, (channel_id, info) in enumerate(filtered.items(), 1):
        blogger_name = info['blogger_name'] or all_channels.get(channel_id, channel_id)
        followers    = info['followers']
        niche        = get_niche(followers)

        log.info(f'  [{idx}/{total}] {blogger_name} | subs={followers:,} | niche={niche}')

        video_ids = get_video_ids(youtube, channel_id, n)
        if len(video_ids) < n / 2:
            log.warning(f'    X Видео: {len(video_ids)} < N/2={n//2}')
            continue

        metrics = get_videos_metrics(youtube, video_ids)
        if not metrics:
            log.warning(f'    X Нет метрик')
            continue

        agg = aggregate(metrics)
        if agg is None:
            log.warning(f'    X avg_views = 0')
            continue

        cpm             = config['CPM_BY_NICHE'].get(niche, 3.0)
        price           = calc_price(agg['avg_views'], cpm)
        campaign_result = calc_campaign_result(agg['avg_views'], agg['engagement_rate'])

        rows.append({
            'platform':        'youtube',
            'blogger_id':      channel_id,
            'blogger_name':    blogger_name,
            'niche':           niche,
            'game_title':      game_title,
            'followers':       followers,
            'avg_views':       agg['avg_views'],
            'engagement_rate': agg['engagement_rate'],
            'content_format':  agg['content_format'],
            'price':           price,
            'campaign_result': campaign_result,
        })
        log.info(
            f'    OK | avg_views={agg["avg_views"]:,.0f} | '
            f'ER={agg["engagement_rate"]:.4f} | '
            f'format={agg["content_format"]} | price=${price}'
        )
        time.sleep(0.3)

    return pd.DataFrame(rows, columns=[
        'platform', 'blogger_id', 'blogger_name', 'niche', 'game_title',
        'followers', 'avg_views', 'engagement_rate',
        'content_format', 'price', 'campaign_result'
    ])


print('Функции загружены')

start = datetime.now()
print('  Gang Beasts - Dataset Collector')

dataset = collect_dataset(CONFIG)

elapsed = int((datetime.now() - start).total_seconds())

if dataset.empty:
    print('\nДатасет пуст. Проверьте API ключ.')
else:
    dataset.to_csv(CONFIG['OUTPUT_FILE'], index=False, encoding='utf-8')
    print(f'\nДатасет сохранён: {CONFIG["OUTPUT_FILE"]}')
    print(f'Строк: {len(dataset)} | Уникальных каналов: {dataset["blogger_id"].nunique()}')
    print(f'Время: {elapsed // 60}м {elapsed % 60}с')
    print('\nРаспределение по нишам (размеру канала):')
    print(dataset.groupby('niche').agg(
        каналов     = ('blogger_name',    'count'),
        avg_subs    = ('followers',        'mean'),
        avg_views   = ('avg_views',        'mean'),
        avg_er      = ('engagement_rate',  'mean'),
        avg_price   = ('price',            'mean'),
    ).round(1))
    dataset

import pandas as pd

df = pd.read_csv("/content/youtube_dataset_gang_beasts.csv")

df

df["blogger_name"]

