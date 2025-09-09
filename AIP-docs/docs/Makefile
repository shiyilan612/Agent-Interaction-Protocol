# Minimal makefile for Sphinx documentation
SPHINXOPTS    =  # 开启警告模式
SPHINXBUILD   = sphinx-build
SOURCEDIR     = docs/source
BUILDDIR      = docs/_build

# 构建HTML文档
html:
	$(SPHINXBUILD) -b html ./source $(BUILDDIR)/html
	@echo
	@echo "HTML文档已构建完成。请打开 $(BUILDDIR)/html/index.html 查看。"

# 清理构建文件
clean:
	rm -rf $(BUILDDIR)/*
