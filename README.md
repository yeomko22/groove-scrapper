# groove-scrapper
groove 프로젝트의 샘플 데이터 베이스 구성을 위해서 soundcloud 서비스에서 데이터를 수집해오는 크롤러입니다.

## Install
```bash
$ pip install -r requirements.txt
```

## Run
1. config를 공유받은 다음 /scrapper/ 폴더 아래에 위치시킵니다.
2. 정보를 수집하고자 하는 soundcloud 유저 아이디를 /scrapper/targets/target.txt에 한 줄에 하나씩 적습니다.
3. sh crawl.sh를 실행합니다.
