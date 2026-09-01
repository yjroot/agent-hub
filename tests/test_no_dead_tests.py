#!/usr/bin/env python3
"""테스트 파일 자체를 검사한다 — **안 도는 테스트**를 두 번 만들었기 때문이다.

tests/*.py 에 메서드를 append 하다 `if __name__ == "__main__":` 블록 **안**에
떨어뜨리면 pytest 가 수집하지 않는다. 초록인데 방어가 없고, 통과 숫자만 보면 모른다
(실측: 그렇게 죽은 테스트 7건 + 1건, 전부 내가 만들었다).

🪤 판정은 들여쓰기 휴리스틱이 아니라 **ast** 로 한다. 가드 *뒤*에 module 레벨 class 로
붙인 것은 정상 수집되는데, 줄 기반 규칙은 그것까지 잡아 과탐이 났다(내 첫 버전).
"""
import ast
import os
import unittest

TESTS = os.path.dirname(os.path.abspath(__file__))


def _dead_defs(path):
    """`if __name__ == "__main__":` 본문 **안**에 갇힌 테스트 정의를 찾는다."""
    tree = ast.parse(open(path).read())
    dead = []
    for node in tree.body:
        if not isinstance(node, ast.If):
            continue
        src = ast.dump(node.test)
        if "__name__" not in src:
            continue
        for inner in ast.walk(node):
            if isinstance(inner, ast.FunctionDef) and inner.name.startswith("test_"):
                dead.append(f"{os.path.basename(path)}:{inner.lineno}: {inner.name}")
            elif isinstance(inner, ast.ClassDef):
                dead.append(f"{os.path.basename(path)}:{inner.lineno}: class {inner.name}")
    return dead


class NoDeadTestsCase(unittest.TestCase):
    def test_no_tests_trapped_in_main_guard(self):
        offenders = []
        for fn in sorted(os.listdir(TESTS)):
            if fn.startswith("test_") and fn.endswith(".py"):
                offenders += _dead_defs(os.path.join(TESTS, fn))
        self.assertEqual(
            offenders, [],
            "__main__ 가드 안에 갇혀 pytest 가 수집하지 않는다 — 클래스 본문으로 옮겨라:\n  "
            + "\n  ".join(offenders))

    def test_guard_itself_discriminates(self):
        """양성 대조 — 가드가 실제로 잡는지 스스로 확인한다(빈 검사 방지)."""
        import tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
            f.write('import unittest\n\nif __name__ == "__main__":\n'
                    '    unittest.main()\n\n    def test_dead(self):\n        pass\n')
            tmp = f.name
        try:
            self.assertTrue(_dead_defs(tmp), "가드가 죽은 테스트를 못 잡는다")
        finally:
            os.unlink(tmp)


if __name__ == "__main__":
    unittest.main(verbosity=2)
