# 서울시 도로소통 Reference Snapshot

이 디렉터리는 서울 열린데이터광장의 `서울시 실시간 도시데이터` API에서 2026-08-28
18:22 KST에 직접 수집한 도로소통 Snapshot과 재현 가능한 manifest를 보관합니다.

- 수집 지역: 광화문·덕수궁, 강남 MICE 관광특구, 여의도
- 원천/검증 통과: 455 / 455 links
- 격리: 0 links
- Parquet 크기: 52,365 bytes
- Parquet SHA-256: `abf3ce20891b6b810ca6abf3372d6c588c30add908b62dfb2ce7d906ece40930`
- 데이터셋: <https://data.seoul.go.kr/dataList/OA-21285/A/1/datasetView.do>
- 이용 조건: 서울특별시 공공데이터 이용정책·공공누리 제1유형(출처표시)

API에 없는 `traffic_volume`과 `reference_speed_kph`는 `NULL`입니다. `observed_at`은
별도 측정시각이 없는 원천 특성상 수집 시각을 서울시 안내 갱신주기인 5분 단위로 내린 값입니다.
원천별 hash, 수집 시각, 지역별 행 수는 `source_manifest.json`에서 확인할 수 있습니다.
