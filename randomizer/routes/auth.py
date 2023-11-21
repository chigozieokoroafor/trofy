from flask import Blueprint, request, url_for, redirect, jsonify, render_template, render_template_string
from werkzeug.security import generate_password_hash, check_password_hash
import jwt
from randomizer.config import users
from pymongo.errors import DuplicateKeyError
import datetime, secrets, random, bson, os
from randomizer.function import Authentication, secret_key
from itsdangerous import URLSafeTimedSerializer, SignatureExpired

template_folder = os.getcwd() + "/templates"
auth = Blueprint("auth_", __name__, url_prefix="/api/auth", template_folder=template_folder)
s = URLSafeTimedSerializer(secret_key)
redirect_url = "https://github.com"

# contaii sighin

@auth.route("/createAccount", methods=["POST"])
def create_account():
    email = request.json.get("email")
    password = request.json.get("password")
    company_name = request.json.get("company_name")
    company_location = request.json.get("company_location")
    

    data = {
        "email":email.lower(),
        "password":generate_password_hash(password, "pbkdf2:sha256", 8),
        "company_name":company_name,
        "company_location":company_location,
        "verified" : False
    }
    try:
        users.insert_one(data)
    except DuplicateKeyError:
        return jsonify(
            message="Email address provided already used",
            success=False,
            data={}
        )
    token = s.dumps(email, salt="email_confirm")
    url = url_for("auth_.confirm_email", token=token, _external=True)
    temp = render_template("verification.html", code=url, year=datetime.datetime.utcnow().year) 
    st = Authentication.mail_send(email, str(temp), "Verification Mail")
    if st["status"] == "success":
        return jsonify(message="Verification link sent to mail", success=True, data={}), 200
    else:
        return jsonify(message="Error while sending mail", success=False, data={}), 400

    

@auth.route("/signin", methods=["POST"])
def signin():
    info = request.json
    email = info.get("email")
    password = info.get("password")
    user_check = users.find({"email":email}).hint("email_1")
    user_list = list(user_check)
    if len(user_list) > 0 :
        user = user_list[0]
        pwd_check = check_password_hash(user["password"], password)
        if pwd_check:
            u_id = str(bson.ObjectId(user["_id"]))
            d = {"id": u_id}
            token = Authentication.generate_access_token(d)
            user["token"] = token
            if user["verified"] == False:
                user["token"] = ""
            user.pop("_id")
            user.pop("password")
            return jsonify({"detail":user, "status":"success"}), 200
        return jsonify({"detail":"Incorrect Details", "status":"fail"}), 401
        
    return jsonify({"detail": f"Account not found for {email}", "status":"fail"}), 404


@auth.route("/emailVerification", methods=["POST"])
def email_verification():
    email = request.json.get("email")
    users.update_one({"email":email}, {"$set":{"verified":False}})
    token = s.dumps(email, salt="email_confirm")
    url = url_for("auth.confirm_email", token=token, _external=True)
    temp = render_template("reset_mail.html", action_url=url) 
    
    st = Authentication.mail_send(email, str(temp), "Verification Mail")
    if st["status"] == "success":
        return jsonify({"detail":{}, "success":True, "message":"use verification link sent to mail provided"}), 200
    else:
        return jsonify({"detail":{}, "success":False, "message":"There was an error while sending verification mails, check back later"}), 400 
 
@auth.route("/confirm_email/<token>")
def confirm_email(token):
    try:
            email = s.loads(token, salt="email_confirm", max_age=300 )
        
        # user = users.find_one({"email":email})
        # if  user["first_timer"]== True:
            # user would be redirected to an email verification successful page
            users.find_one_and_update({"email":email},{"$set":{"verified":True}})
            return redirect(redirect_url), 302 # change this
        # else:
        #     url = url_for("providers.auth.res_pass", token=token, _external=True)
        #     return render_template("pwd_reset.html", post_url=url),  200
            # user would be redirected to password reset page
        
            
    except SignatureExpired:
        return render_template_string("<h1> link expired </h1>")


@auth.route("/updatePassword", methods=["POST"])
@Authentication.token_required
def newPassword():
    token = request.headers.get("Authorization")
    data = jwt.decode(token, secret_key,algorithms=["HS256"])    
    new_password = request.json.get("newPassword")
    
    try:
        email = data["email"]
    except KeyError as e:
        return jsonify({"detail":"Incorrect token provided", "status":"error"}), 401
    user_cursor = users.find({"email":email}).hint("email_1")
    user_list = list(user_cursor)
    user = user_list[0]
    choice_length = random.choice([16,32,64])
    new_hash = generate_password_hash(new_password,salt_length=choice_length)
    log_key = secrets.token_hex(32)
    users.find_one_and_update({"_id":user["_id"]}, {"$set":{"pwd":new_hash, "login_key":log_key}})
    
    return jsonify({"detail":"Password Updated Successfully", "status":"success"}), 200


def signin():
    pass

def  reset_password():
    pass


def verify_password():
    pass