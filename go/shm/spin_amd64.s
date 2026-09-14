//go:build amd64

TEXT ·Procyield(SB),7,$0
    MOVL cycles+0(FP), AX
again:
    PAUSE
    SUBL $1, AX
    JNZ again
    RET
