# Max Concurrency Search Report

Generated at: 2026-04-30 08:58:04 CST

## Test Profile

- transport: `ws`
- url: `ws://www.kemande.cn:10222/ws/v1/vad`
- wav: `/home/wuhao@kemande.cn/work/xiaozhi-vad-service/test.wav`
- realtime: `false`
- duration per run: `60s`
- start concurrency: `50`
- max concurrency: `1000`
- ramp factor: `2`
- allowed session failures: `0`
- allowed frame failures: `0`

## Result

- max stable concurrency: `974`
- first unstable concurrency: `975`
- best stable summary: sessions=974/974 failed=0, frames=530830/530830 failed=0, session_rate=12.42, frame_rate=6766.72, avg=131.278557ms, p95=258.659805ms, p99=302.401889ms

## Run Details

| Concurrency | Status | Sessions | Frames | Sessions/s | Frames/s | Avg Latency | P95 | P99 |
|---:|---|---|---|---:|---:|---:|---:|---:|
| 50 | stable | 722/722 failed=0 | 393490/393490 failed=0 | 11.50 | 6267.95 | 7.788197ms | 10.854229ms | 15.435599ms |
| 100 | stable | 931/931 failed=0 | 507395/507395 failed=0 | 14.61 | 7964.14 | 12.029797ms | 18.080959ms | 23.827538ms |
| 200 | stable | 1000/1000 failed=0 | 545000/545000 failed=0 | 14.20 | 7737.84 | 25.30348ms | 35.8931ms | 41.720097ms |
| 400 | stable | 867/867 failed=0 | 472515/472515 failed=0 | 13.04 | 7104.51 | 52.047942ms | 72.321654ms | 88.033197ms |
| 800 | stable | 918/918 failed=0 | 500310/500310 failed=0 | 12.55 | 6839.19 | 100.760187ms | 147.156067ms | 251.945383ms |
| 1000 | unstable | 986/31735 failed=30749 | 537370/537370 failed=0 | 11.54 | 6289.59 | 143.356702ms | 265.974275ms | 318.606664ms |
| 900 | stable | 900/900 failed=0 | 490500/490500 failed=0 | 11.93 | 6499.70 | 126.875054ms | 203.289652ms | 299.137283ms |
| 950 | stable | 950/950 failed=0 | 517750/517750 failed=0 | 12.09 | 6590.06 | 131.666476ms | 250.322719ms | 305.008239ms |
| 975 | unstable | 975/982 failed=7 | 531375/531375 failed=0 | 12.28 | 6694.17 | 132.922304ms | 258.205187ms | 301.410669ms |
| 962 | stable | 962/962 failed=0 | 524290/524290 failed=0 | 12.47 | 6798.67 | 129.327225ms | 249.130507ms | 293.667052ms |
| 968 | stable | 968/968 failed=0 | 527560/527560 failed=0 | 12.48 | 6802.03 | 129.983117ms | 254.083487ms | 295.179512ms |
| 971 | stable | 971/971 failed=0 | 529195/529195 failed=0 | 12.30 | 6702.47 | 132.341875ms | 262.389729ms | 299.26789ms |
| 973 | stable | 973/973 failed=0 | 530285/530285 failed=0 | 12.38 | 6744.62 | 131.33667ms | 250.378701ms | 292.202178ms |
| 974 | stable | 974/974 failed=0 | 530830/530830 failed=0 | 12.42 | 6766.72 | 131.278557ms | 258.659805ms | 302.401889ms |
