"""x86_64经典BPF有限白名单；无依赖下载，不提供通用syscall配置。"""

import struct

ARCH = 0xC000003E
ALLOW = 0x7FFF0000
DENY = 0x50000 | 1
KILL = 0x80000000
# 文件/内存/时钟/信号/有限fork与Python运行所需；socket/mount/ptrace等默认拒绝。
ALLOWED = (
    0,
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    17,
    18,
    19,
    20,
    21,
    22,
    23,
    24,
    25,
    28,
    32,
    33,
    35,
    36,
    37,
    38,
    39,
    57,
    58,
    59,
    60,
    61,
    62,
    63,
    72,
    73,
    74,
    75,
    76,
    77,
    78,
    79,
    80,
    81,
    82,
    83,
    84,
    85,
    86,
    87,
    88,
    89,
    90,
    91,
    95,
    96,
    97,
    98,
    99,
    100,
    102,
    104,
    107,
    108,
    110,
    111,
    112,
    115,
    116,
    117,
    118,
    119,
    120,
    121,
    124,
    125,
    131,
    132,
    137,
    138,
    140,
    141,
    145,
    146,
    147,
    148,
    149,
    158,
    160,
    186,
    200,
    201,
    202,
    203,
    204,
    213,
    217,
    218,
    219,
    221,
    228,
    229,
    230,
    231,
    232,
    233,
    234,
    235,
    247,
    257,
    258,
    260,
    261,
    262,
    263,
    264,
    265,
    266,
    267,
    268,
    269,
    270,
    271,
    273,
    280,
    281,
    292,
    293,
    295,
    296,
    302,
    309,
    316,
    318,
    332,
    334,
    436,
    439,
)


def program() -> bytes:
    instructions = []

    def emit(code, jt, jf, k):
        instructions.append((code, jt, jf, k))

    # arch和x32号先拒绝，不能利用兼容ABI绕过同一策略。
    emit(0x20, 0, 0, 4)
    emit(0x15, 1, 0, ARCH)
    emit(0x06, 0, 0, KILL)
    emit(0x20, 0, 0, 0)
    emit(0x45, 0, 1, 0x40000000)
    emit(0x06, 0, 0, DENY)
    # clone3返回ENOSYS，让受限fork使用可检查flags的clone。
    emit(0x15, 0, 1, 435)
    emit(0x06, 0, 0, 0x50000 | 38)
    # clone:禁止所有namespace位；其余受cgroup/rlimit约束。
    emit(0x15, 0, 4, 56)
    emit(0x20, 0, 0, 16)
    emit(0x45, 0, 1, 0x7E020080)
    emit(0x06, 0, 0, DENY)
    emit(0x06, 0, 0, ALLOW)
    # mknod/mknodat仅FIFO类型可用；设备创建始终拒绝。
    for nr, offset in [(133, 24), (259, 32)]:
        emit(0x15, 0, 5, nr)
        emit(0x20, 0, 0, offset)
        emit(0x54, 0, 0, 0xF000)
        emit(0x15, 1, 0, 0x1000)
        emit(0x06, 0, 0, DENY)
        emit(0x06, 0, 0, ALLOW)
    # prctl只能读取状态或向安全方向设置dumpable/NNP。
    branch = []

    def b(code, jt, jf, k):
        branch.append((code, jt, jf, k))

    b(0x20, 0, 0, 16)
    for nr in [3, 21, 39]:
        b(0x15, 0, 1, nr)
        b(0x06, 0, 0, ALLOW)
    b(0x15, 0, 4, 4)
    b(0x20, 0, 0, 24)
    b(0x15, 1, 0, 0)
    b(0x06, 0, 0, DENY)
    b(0x06, 0, 0, ALLOW)
    b(0x15, 0, 4, 38)
    b(0x20, 0, 0, 24)
    b(0x15, 1, 0, 1)
    b(0x06, 0, 0, DENY)
    b(0x06, 0, 0, ALLOW)
    b(0x06, 0, 0, DENY)
    emit(0x15, 0, len(branch), 157)
    instructions.extend(branch)
    for nr in ALLOWED:
        emit(0x15, 0, 1, nr)
        emit(0x06, 0, 0, ALLOW)
    emit(0x06, 0, 0, DENY)
    return b"".join(struct.pack("<HBBI", *x) for x in instructions)
