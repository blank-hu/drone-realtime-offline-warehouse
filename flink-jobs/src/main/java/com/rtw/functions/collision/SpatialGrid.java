package com.rtw.functions.collision;

import java.util.ArrayList;
import java.util.List;

public class SpatialGrid {

    // 把 (ix,iy,iz) 编成一个 long key。实现不唯一，稳定即可。
    public static long cellKey(int ix, int iy, int iz) {
        long x = (long) ix;
        long y = (long) iy;
        long z = (long) iz;
        // 简单混合：避免冲突太多即可
        long h = 1469598103934665603L;
        h = (h ^ x) * 1099511628211L;
        h = (h ^ y) * 1099511628211L;
        h = (h ^ z) * 1099511628211L;
        return h;
    }

    // 27 邻域（含自己）
    public static List<int[]> neighbors27() {
        List<int[]> res = new ArrayList<>(27);
        for (int dx=-1; dx<=1; dx++)
            for (int dy=-1; dy<=1; dy++)
                for (int dz=-1; dz<=1; dz++)
                    res.add(new int[]{dx,dy,dz});
        return res;
    }
}
