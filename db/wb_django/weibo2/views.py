from django.shortcuts import render

# Create your views here.
def index(request):
    return None


def tendency(request):
    # 返回趋势
    keyword = request.GET.get('keyword','')
    return None