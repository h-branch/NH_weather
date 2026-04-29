import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import geopandas as gpd
from matplotlib.colors import ListedColormap, BoundaryNorm, Normalize
from matplotlib.cm import ScalarMappable
plt.rcParams['font.family'] = 'Malgun Gothic'
plt.rcParams['axes.unicode_minus'] = False


# 1
sido='C:/Users/lhj15/OneDrive/문서/[NH]/이상기후대응팀/#map_basic/ramp_gis/sido_ramp.shp'
sgg='C:/Users/lhj15/OneDrive/문서/[NH]/이상기후대응팀/#map_basic/1. 시군구/SGG.shp'
gpd_sido=gpd.read_file(sido)
gpd_sgg=gpd.read_file(sgg)
if gpd_sido.crs != gpd_sgg.crs:
    gpd_sgg = gpd_sgg.to_crs(gpd_sido.crs)

# 2
prcp_color=['#00c8ff', '#009bf5', '#0049f5', #청색
            '#00ff00', '#00be00', '#008c00', '#005a00', #녹색
            '#ffff00', '#ffdd1f', '#f9cb00', '#e0b900', '#ccaa00', #황색
            '#ff6600', '#ff3300', '#d20000', '#b40000', #적색
            '#dfa9ff', '#c969ff', '#b429ff', '#9300e4', #자색
            '#b3b4de', '#4c4eb1', '#000390' #남색
]
colormap_rain=ListedColormap(prcp_color).with_extremes(over='#333333', under='#ffffff')
colormap_rain.set_bad([0,0,0,0])
bounds=np.array([
    0., 0.1, 0.5, 1.,
    2., 3., 4., 5., 
    6., 7., 8., 9., 10., 
    15., 20., 25., 30., 
    40., 50., 60., 70., 
    90., 110., 150.
])
norm=BoundaryNorm(boundaries=bounds, ncolors=len(colormap_rain.colors))
colormap_rain
ticks=bounds[:]

# 3. 특별재난지역 선포 현황
bomb = [
    '포천시'
]
bomb_all = gpd_sgg[gpd_sgg['SGG'].isin(bomb)]

fire = [
    '산청군','울주군','의성군','하동군','안동시','청송군','영양군','영덕군'
]
fire_all = gpd_sgg[gpd_sgg['SGG'].isin(fire)]

jul = [
    '가평군','북구','서산시','예산군','천안시','공주시','아산시','당진시','부여군',
    '청양군','홍성군','나주시','담양군','함평군','청도군','산청군','합천군','진주시','의령군','하동군','함양군',
    '광산구','청주시','서천군','광양시','구례군','화순군','영광군','신안군','밀양시','거창군','세종특별자치시'
]
jul_all = gpd_sgg[gpd_sgg['SGG'].isin(jul)]

aug = [
    '무안군','함평군'
]
aug_all = gpd_sgg[gpd_sgg['SGG'].isin(aug)]

# 4. 동명이칭(광주광역시 북구)
jul_no_bukgu = [x for x in jul if x != "북구"]
jul_all = gpd_sgg[
    (gpd_sgg["SGG"].isin(jul_no_bukgu)) |
    (
        (gpd_sgg["SGG"] == "북구") &
        (gpd_sgg["CODE"].astype(str).str[:2] == "29")
    )
].copy()

# 5
bomb_all['rain']=5.
fire_all['rain']=20.
jul_all['rain']=0.5
aug_all['rain']=0.1

# 6
fig, ax = plt.subplots(figsize=(10, 10))
gpd_sgg.plot(ax=ax, facecolor='none', edgecolor='gray', linewidth=0.2, zorder=1)

bomb_all.plot(ax=ax, column='rain', cmap=colormap_rain, norm=norm)
jul_all.plot(ax=ax, column='rain', cmap=colormap_rain, norm=norm)
aug_all.plot(ax=ax, column='rain', cmap=colormap_rain, norm=norm)
fire_all.plot(ax=ax, column='rain', cmap=colormap_rain, norm=norm)

gpd_sido.boundary.plot(ax=ax, color='black', linewidth=1.0, zorder=2)

sm=ScalarMappable(cmap=colormap_rain, norm=norm)
sm.set_array([])

cbar=fig.colorbar(sm, ax=ax, fraction=0.03, pad=0.02)
cbar.set_ticks(ticks)
cbar.set_label('Rain (㎜)')

ax.set_axis_off()
plt.tight_layout()
plt.show()
