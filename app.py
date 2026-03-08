import os
from flask import Flask, render_template, request, redirect, url_for, session, flash
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from search_engine_backend import SearchEngine


app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'your-secret-key-here')

# 配置Flask-Login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'


users = {
    'admin': generate_password_hash('password'),
    'user1': generate_password_hash('user123')
}


class User(UserMixin):
    def __init__(self, id):
        self.id = id

# 用户加载回调
@login_manager.user_loader
def load_user(user_id):
    if user_id in users:
        return User(user_id)
    return None

# 初始化搜索引擎
engine = SearchEngine()


@app.route('/')
def index():
    return render_template('index.html')

# 搜索路由
@app.route('/search', methods=['GET'])
def search():
    query = request.args.get('q', '')
    if not query:
        return redirect(url_for('index'))
    
    
    page = int(request.args.get('page', 1))
    per_page = 10
    
    
    results = engine.vector_space_search(query)
    total_results = len(results)
    
    
    start = (page - 1) * per_page
    end = start + per_page
    paginated_results = results[start:end]
    
    # 计算总页数
    total_pages = (total_results + per_page - 1) // per_page
    
    return render_template('search_results.html', 
                           query=query,
                           results=paginated_results,
                           total_results=total_results,
                           page=page,
                           total_pages=total_pages)

# 登录路由
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        if username in users and check_password_hash(users[username], password):
            user = User(username)
            login_user(user)
            flash('登录成功', 'success')
            return redirect(url_for('index'))
        else:
            flash('用户名或密码错误', 'error')
    
    return render_template('login.html')

# 注册路由
@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        if username in users:
            flash('用户名已存在', 'error')
            return redirect(url_for('register'))
        
        # 添加新用户
        users[username] = generate_password_hash(password)
        flash('注册成功，请登录', 'success')
        return redirect(url_for('login'))
    
    return render_template('register.html')

# 登出路由
@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash('已登出', 'info')
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True)    