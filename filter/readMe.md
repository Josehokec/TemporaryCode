

# Cuckoo Filter & Shrink Filter

## 一些关键问题解答
1.实现并发的filter没有使用先试探再插入的方法，不用的原因是我们踢出的时候只踢一个桶，
之前方法高效的原因之一是锁的数量太少了

2.SFTable插入的时候会从低到高插入，注意到SFTable只会插入一次，
之后会不断收缩，因此插入算法判断第一个的interval_marker是0确实可以直接插入，
因为这只会发生ShrinkFilter构造阶段，而且update不涉及到插入

**目前ShrinkFilter还不支持并发插入**

核心就是槽中信息存储<fp, interval_marker, hit_marker>

Shrink filter支持
- 压缩：把桶的长度砍半
- 收缩（更新）：把时间戳范围收的越来越紧
- 目前支持四个配置（因为要保证4个槽长度是64的倍数）

