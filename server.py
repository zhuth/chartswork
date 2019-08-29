import random
from pyecharts.charts import Page
import pyecharts.charts
from flask import Flask, render_template
from pyecharts import options as opts
from jinja2 import Markup, Environment, FileSystemLoader
from pyecharts.globals import CurrentConfig

from helpers import *

range_color = ['#313695', '#4575b4', '#74add1', '#abd9e9', '#e0f3f8', '#ffffbf',
                '#fee090', '#fdae61', '#f46d43', '#d73027', '#a50026']

app = Flask(__name__, static_folder="templates")

CurrentConfig.GLOBAL_ENV = Environment(loader=FileSystemLoader("./templates"))


@app.route("/favicon.ico")
def favicon_404():
    return '', 404


@app.route("/<scheme>")
def render_scheme(scheme):

    y = yaml.safe_load(open(f'schemata/{scheme}.yaml'))

    page = Page()

    for comp in y:
        data = DataSources[comp['datasource']](**comp['query'])
        chart_c = getattr(pyecharts.charts, comp['type'])
        chart = chart_c().set_global_opts(
                title_opts=opts.TitleOpts(comp['title']),
                # visualmap_opts=opts.VisualMapOpts(range_color=range_color)
        )
        chart.add("", [i[0] for i in data.values],
            xaxis3d_opts=opts.Axis3DOpts(type_="category"),
            yaxis3d_opts=opts.Axis3DOpts(type_="category"),
            zaxis3d_opts=opts.Axis3DOpts(type_="value"),)
        page.add(chart)

    return Markup(page.render_embed())


if __name__ == "__main__":
    app.run(debug=True)
