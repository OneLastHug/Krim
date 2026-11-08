"""EgressPolicy 纯校验测试：URL 规则与地址审批规则（无网络）。"""

from krim_agent.network.egress_policy import EgressPolicy, EgressViolation, normalize_host

PUBLIC = EgressPolicy({"api.example.com"}, {443})


def test_normalize_host_strips_trailing_dot_and_lowercases() -> None:
    assert normalize_host("API.Example.COM.") == "api.example.com"


def test_https_target_accepted_with_default_port() -> None:
    target = PUBLIC.validate_target("https://api.example.com/x")
    assert target.scheme == "https"
    assert target.host == "api.example.com"
    assert target.connect_host == "api.example.com"
    assert target.port == 443


def test_host_match_is_exact_and_normalized() -> None:
    target = PUBLIC.validate_target("https://API.Example.COM./path")
    assert target.host == "api.example.com"


def test_unicode_host_is_canonicalized_to_idna_form() -> None:
    # 非 ASCII 条目与 URL 统一规范化为报文用的 punycode 形式再匹配。
    idn = EgressPolicy({"täst.de"}, {443})
    target = idn.validate_target("https://täst.de/")
    assert target.host == "xn--tst-qla.de"
    assert target.connect_host == "xn--tst-qla.de"
    # httpx 会把 unicode host 规范化为 punycode，两种写法命中同一条目。
    assert idn.validate_target("https://xn--tst-qla.de/").host == "xn--tst-qla.de"


def test_idna_folding_ambiguity_is_rejected() -> None:
    # 有损折叠（nameprep/NFKC）：软连字符、零宽、全角、ß——不同写法同形，拒绝。
    # 注意：soft_hyphen_host/zero_width_host 两个字符串字面量里是真实的
    # U+00AD 软连字符与 U+200B 零宽空格字符（显示上不可见），为有意内容。
    soft_hyphen_host = "t­äst.de"
    zero_width_host = "ex​ample.com"
    for entry in (soft_hyphen_host, "ｔäst.de", "ß.de", zero_width_host):
        try:
            EgressPolicy({entry}, {443})
        except EgressViolation:
            continue
        raise AssertionError(f"idna folding entry must be rejected: {entry!r}")
    idn = EgressPolicy({"täst.de"}, {443})
    for url in (f"https://{soft_hyphen_host}/", "https://ｔäst.de/", f"https://{zero_width_host}/"):
        try:
            idn.validate_target(url)
        except EgressViolation:
            continue
        raise AssertionError(f"idna folding url must be rejected: {url!r}")
    # punycode 字面量条目保留（MCP 文档写法之一）。
    puny = EgressPolicy({"xn--tst-qla.de"}, {443})
    assert puny.validate_target("https://xn--tst-qla.de/").host == "xn--tst-qla.de"


def test_subdomain_is_not_implicitly_allowed() -> None:
    try:
        PUBLIC.validate_target("https://x.api.example.com/")
    except EgressViolation:
        pass
    else:
        raise AssertionError("subdomain must not match an exact host entry")


def test_other_host_rejected() -> None:
    try:
        PUBLIC.validate_target("https://other.example.com/")
    except EgressViolation as error:
        assert "other.example.com" in str(error)
    else:
        raise AssertionError("unlisted host must be rejected")


def test_only_https_is_allowed_by_default() -> None:
    for url in (
        "http://api.example.com/",
        "ftp://api.example.com/",
        "wss://api.example.com/",
        "//api.example.com/",
    ):
        try:
            PUBLIC.validate_target(url)
        except EgressViolation:
            continue
        raise AssertionError(f"non-https scheme must be rejected: {url}")


def test_userinfo_is_rejected() -> None:
    for url in (
        "https://user@api.example.com/",
        "https://user:pw-secret-123@api.example.com/",
    ):
        try:
            PUBLIC.validate_target(url)
        except EgressViolation:
            continue
        raise AssertionError(f"userinfo must be rejected: {url}")


def test_query_and_fragment_rejected_anywhere_in_url() -> None:
    for url in (
        "https://api.example.com/?a=b",
        "https://api.example.com/#frag",
        "https://api.example.com/x?#",
        "?https://api.example.com/",
        "https://api.example.com/#",
    ):
        try:
            PUBLIC.validate_target(url)
        except EgressViolation:
            continue
        raise AssertionError(f"query/fragment must be rejected: {url}")


def test_whitespace_and_control_characters_rejected() -> None:
    for url in (
        " https://api.example.com/",
        "https://api.example.com/ ",
        "https://api.example.com/a b",
        "https://api.example.com/\x00",
        "https://api.example.com/\n",
    ):
        try:
            PUBLIC.validate_target(url)
        except EgressViolation:
            continue
        raise AssertionError(f"whitespace/control chars must be rejected: {url!r}")


def test_unicode_control_characters_rejected() -> None:
    # C1 控制区（U+0080-U+009F）与 DEL 不属于空白，但同样是 Cc 控制字符。
    for url in (
        "https://api.example.com/\x7f",
        "https://api\x7f.example.com/",
        "https://api\x9f.example.com/",
        "https://api.example\x85.com/",
    ):
        try:
            PUBLIC.validate_target(url)
        except EgressViolation:
            continue
        raise AssertionError(f"control characters must be rejected: {url!r}")
    try:
        EgressPolicy({"api\x9f.example.com"}, {443})
    except EgressViolation:
        pass
    else:
        raise AssertionError("control characters must be rejected in host entries")


def test_missing_host_rejected() -> None:
    for url in ("https://", "https:///path"):
        try:
            PUBLIC.validate_target(url)
        except EgressViolation:
            continue
        raise AssertionError(f"empty host must be rejected: {url}")


def test_port_rules() -> None:
    explicit = EgressPolicy({"api.example.com"}, {8443})
    assert explicit.validate_target("https://api.example.com:8443/").port == 8443
    for policy, url in (
        (PUBLIC, "https://api.example.com:8443/"),
        (explicit, "https://api.example.com:443/"),
        # 未写端口时使用方案默认端口，默认端口必须也在白名单内。
        (explicit, "https://api.example.com/"),
    ):
        try:
            policy.validate_target(url)
        except EgressViolation:
            continue
        raise AssertionError(f"disallowed port must be rejected: {url}")


def test_invalid_port_forms_rejected() -> None:
    for url in (
        "https://api.example.com:0/",
        "https://api.example.com:70000/",
        "https://api.example.com:abc/",
    ):
        try:
            PUBLIC.validate_target(url)
        except EgressViolation:
            continue
        raise AssertionError(f"invalid port must be rejected: {url}")


def test_loopback_authorization_cannot_come_from_url_content() -> None:
    # host 白名单即使包含 IP 字面量，地址层也拒绝 loopback：
    # 授权只存在于 authorize_loopback 构造，与被请求的 URL 无关。
    literal_policy = EgressPolicy({"127.0.0.1"}, {443})
    target = literal_policy.validate_target("https://127.0.0.1/")
    assert target.host == "127.0.0.1"
    try:
        literal_policy.approve_addresses(("127.0.0.1",))
    except EgressViolation:
        pass
    else:
        raise AssertionError("loopback address must be rejected without clearance")


def test_approve_addresses_accepts_public_addresses_in_order() -> None:
    assert PUBLIC.approve_addresses(("4.5.6.7", "1.2.3.4", "4.5.6.7")) == ("4.5.6.7", "1.2.3.4")
    assert PUBLIC.approve_addresses(("1.2.3.4",)) == ("1.2.3.4",)


def test_approve_addresses_unwraps_ipv4_mapped_public_address() -> None:
    assert PUBLIC.approve_addresses(("::ffff:1.2.3.4",)) == ("1.2.3.4",)


def test_approve_addresses_rejects_unsafe_and_mixed_inputs() -> None:
    unsafe = (
        "10.0.0.1",
        "172.16.0.1",
        "192.168.0.1",
        "127.0.0.1",
        "169.254.1.1",
        "100.64.0.1",
        "0.0.0.0",
        "240.0.0.1",
        "224.0.0.1",
        "255.255.255.255",
        "198.18.0.1",
        "192.0.2.1",
        "198.51.100.1",
        "203.0.113.1",
        "::1",
        "::",
        "fe80::1",
        "fc00::1",
        "ff02::1",
        "2001:db8::1",
        "64:ff9b::102:304",
        "2002::102:304",
        "::ffff:127.0.0.1",
        "not-an-ip",
        "",
    )
    for address in unsafe:
        try:
            PUBLIC.approve_addresses((address,))
        except EgressViolation:
            continue
        raise AssertionError(f"unsafe address must be rejected: {address}")
    # mixed public/private：任一地址不安全即整体拒绝。
    try:
        PUBLIC.approve_addresses(("1.2.3.4", "10.0.0.1"))
    except EgressViolation:
        pass
    else:
        raise AssertionError("mixed public/private dns must be rejected")
    # mixed mapped public/mapped loopback：解包后按 loopback 规则拒绝。
    try:
        PUBLIC.approve_addresses(("::ffff:1.2.3.4", "::ffff:127.0.0.1"))
    except EgressViolation:
        pass
    else:
        raise AssertionError("mixed mapped loopback must be rejected")
    try:
        PUBLIC.approve_addresses(())
    except EgressViolation:
        pass
    else:
        raise AssertionError("empty resolution must be rejected")


def test_loopback_clearance_only_via_trusted_constructor() -> None:
    loopback = EgressPolicy.authorize_loopback({"localhost"}, {8399})
    assert loopback.allows_loopback is True
    assert PUBLIC.allows_loopback is False

    target = loopback.validate_target("http://localhost:8399/")
    assert target.scheme == "http"
    assert loopback.approve_addresses(("127.0.0.1", "::1")) == ("127.0.0.1", "::1")

    # cleartext http 仍只允许 loopback 目标。
    try:
        loopback.validate_target("http://api.example.com/")
    except EgressViolation:
        pass
    else:
        raise AssertionError("cleartext http to public host must stay rejected")

    # 授权不是全局放开：mixed loopback/public 仍拒绝。
    try:
        loopback.approve_addresses(("127.0.0.1", "1.2.3.4"))
    except EgressViolation:
        pass
    else:
        raise AssertionError("mixed loopback/public dns must be rejected")

    # 授权策略对非白名单 host 依旧拒绝。
    try:
        loopback.validate_target("http://localhost:9999/")
    except EgressViolation:
        pass
    else:
        raise AssertionError("disallowed port must stay rejected under clearance")


def test_constructor_validates_entries() -> None:
    for hosts, ports in (
        ({"*.example.com"}, {443}),
        ({""}, {443}),
        ({"bad/host"}, {443}),
        ({"user@host"}, {443}),
        ({"host:99"}, {443}),
        ({"ok.example.com"}, {0}),
        ({"ok.example.com"}, {70000}),
        ({"ok.example.com"}, {True}),
    ):
        try:
            EgressPolicy(hosts, ports)
        except EgressViolation:
            continue
        raise AssertionError(f"invalid policy entries must be rejected: {hosts} {ports}")
