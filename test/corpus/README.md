# Tranfi Native Fuzz Seeds

These files are curated seed inputs for the native libFuzzer harnesses. The
default Makefile targets use these tracked seeds as read-only inputs and write
new generated corpus units into the ignored `corpus/` work directory.

Keep seeds small, text-only, and focused on parser/runtime boundaries:
quoting, nesting, selectors, schema syntax, side-channel policy options, record
caps, and streaming chunk boundaries. Do not copy proprietary or third-party
corpora here.
