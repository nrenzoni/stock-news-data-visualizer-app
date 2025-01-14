def add_horizontal_line(fig, x0, x1, y_axis_intercept):
    fig.add_shape(
        type='line',
        x0=x0,
        y0=y_axis_intercept,
        x1=x1,
        y1=y_axis_intercept,
        line=dict(
            color='red',
            width=2,
            dash='dash'
        )
    )
