import os
import pandas as pd
from bokeh.plotting import figure
from bokeh.io import export_png

lset_path = os.path.join(os.path.dirname(__file__), '../../results/lset')
image_dir = os.path.join(os.path.dirname(
    __file__), '../../thalassophilia/docs/images')
lset = pd.read_json(lset_path, lines=True)

lu = lset[lset['primary_mean'].isnull()]
lp = lset[~lset['primary_mean'].isnull()]


def save_plot(p, name):
    p.legend.location = 'top_left'
    p.background_fill_color = None
    p.border_fill_color = None
    p.toolbar_location = None
    export_png(p, filename=os.path.join(image_dir, f'{name}.png'))


p = figure(title="Mean time to delivery", x_axis_label='# nodes',
           y_axis_label='ms', y_range=[0, 25])
p.line(lu['nodes'], lu['overall_mean'] / 1000,
       legend_label='Uniform', color='blue')
p.line(lp['nodes'], lp['primary_mean'] / 1000,
       legend_label='Primaries', color='orange')
p.line(lp['nodes'], lp['secondary_mean'] / 1000,
       legend_label='Secondaries', color='green')
save_plot(p, 'gossip_mean_time_to_delivery')

p = figure(title="p50 time to delivery", x_axis_label='# nodes',
           y_axis_label='ms', y_range=[0, 11])
p.line(lu['nodes'], lu['overall_p50'] / 1000,
       legend_label='Uniform', color='blue')
p.line(lp['nodes'], lp['primary_p50'] / 1000,
       legend_label='Primaries', color='orange')
p.line(lp['nodes'], lp['secondary_p50'] / 1000,
       legend_label='Secondaries', color='green')
save_plot(p, 'gossip_p50_time_to_delivery')

p = figure(title="p90 time to delivery", x_axis_label='# nodes',
           y_axis_label='ms', y_range=[0, 50])
p.line(lu['nodes'], lu['overall_p90'] / 1000,
       legend_label='Uniform', color='blue')
p.line(lp['nodes'], lp['primary_p90'] / 1000,
       legend_label='Primaries', color='orange')
p.line(lp['nodes'], lp['secondary_p90'] / 1000,
       legend_label='Secondaries', color='green')
save_plot(p, 'gossip_p90_time_to_delivery')
