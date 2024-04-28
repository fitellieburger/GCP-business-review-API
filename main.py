from flask import Flask, request, jsonify
from google.cloud import datastore

"""
GLOBALS
"""

# entities
BUSINESSES = 'businesses'
USERS = 'users'
OWNERS = 'owners'
REVIEWS = 'reviews'

# errors
ERROR_MISSING = {"Error": "The request body is missing at least one of the required attributes"}
ERROR_NO_REVIEW = {"Error": "No review with this review_id exists"}
ERROR_NO_BUS = {"Error": "No business with this business_id exists"}
ERROR_CONFLICT = {"Error": "You have already submitted a review for this business. You can update your previous review, or delete it and submit a new review"
                 }

app = Flask(__name__)
client = datastore.Client()


"""
SUPPORT FUNCTIONS: VALIDATION
"""


# validates content for POST and PUT requests for BUSINESSES
def validate_content(content):
    if 'name' not in content or 'street_address' not in content or\
     'city' not in content or 'state' not in content or 'zip_code'\
       not in content or 'owner_id' not in content:
        return False

    return True


# validates content for POST and PUT requests for REVIEWS
def validate_review(content):
    if 'user_id' not in content or 'business_id' not in content or\
     'stars' not in content:
        return False

    return True


@app.route('/')
def index():
    return 'Hello, this is the main pade for dburger\'s API. Please enjoy your\
      CRUD operations today.'


"""
Businesses entity
auto-assigned (datacloud) id
owner_id
name
street_address
city
state
zip_code

"""


# POST route creates a business
# verifies response
# responds  when the a response is missing
# store information in datastore, associated with user creating the business
@app.route('/' + BUSINESSES, methods=['POST'])
def post_businesses():

    content = request.get_json()

    # if content is not valid, return 400
    if validate_content(content) is False:
        return (ERROR_MISSING, 400)

    # create datastore entity
    new_entity = datastore.Entity(key=client.key(BUSINESSES))
    new_entity.update({
        'owner_id': content['owner_id'],
        'name': content['name'],
        'street_address': content['street_address'],
        'city': content['city'],
        'state': content['state'],
        'zip_code': content['zip_code']
    })

    # save the new entity in datastore
    client.put(new_entity)

    # capture assigned id
    new_entity['id'] = new_entity.key.id

    # return a 201 response with newly created data
    return (jsonify(new_entity), 201)


# GET route returns a list of all businesses
@app.route('/' + BUSINESSES, methods=['GET'])
def get_businesses():

    # query for all businesses
    query = client.query(kind=BUSINESSES)

    # creates list of results
    results = list(query.fetch())

    # add ids for ease of use
    for r in results:
        r['id'] = r.key.id

    return (results, 200)


# GET route returns all businesses associated with a specific owner
# Gets a list of all businesses for that owner
@app.route('/' + OWNERS + '/<int:owner_id>/' + BUSINESSES, methods=['GET'])
def get_businesses_for_owner(owner_id):

    # query for all businesses
    query = client.query(kind=BUSINESSES)
    # add filter
    query.add_filter('owner_id', '=', owner_id)

    # creates list of results
    results = list(query.fetch())

    # add ids for ease of use
    for r in results:
        r['id'] = r.key.id

    return (results, 200)


# GET route returns information for a specific business
# returns 404 ERROR if business does not exist
@app.route('/' + BUSINESSES + '/<int:business_id>', methods=['GET'])
def get_business(business_id):

    # fetch business from datastore
    business_key = client.key(BUSINESSES, business_id)
    business = client.get(key=business_key)

    # if business does not exist, return ERROR
    if business is None:
        return (ERROR_NO_BUS, 404)

    business['id'] = business.key.id

    return (jsonify(business), 200)


# PUT route updates a business given a business_id
# returns 404 ERROR if business does not exist
# returns 400 ERROR if attributes are missing from request
@app.route('/' + BUSINESSES + '/<int:business_id>', methods=['PUT'])
def put_business(business_id):

    # fetch business from datastore
    content = request.get_json()

    business_key = client.key(BUSINESSES, business_id)
    business = client.get(key=business_key)

    # if business does not exist, return ERROR
    if business is None:
        return (ERROR_NO_BUS, 404)
    business['id'] = business.key.id

    # if attributes are missing in request body, return ERROR
    if validate_content(content) is False:
        return (ERROR_MISSING, 400)
    
    # update business
    business.update({
                   'name': content['name'],
                    'street_address': content['street_address'],
                    'city': content['city'],
                    'state': content['state'],
                    'zip_code': content['zip_code']
                    })

    client.put(business)

    # confirm business is
    business['id'] = business.key.id

    # return updated business entity in body
    return business


# DELETE route deletes a business with a given id
# returns 404 ERROR if business does not exist
# deletes all associated reviews
@app.route('/' + BUSINESSES + '/<int:business_id>', methods=['DELETE'])
def delete_business(business_id):

    # access business in datastore
    business_key = client.key(BUSINESSES, business_id)
    business = client.get(key=business_key)

    # if no business is found
    if business is None:
        return (ERROR_NO_BUS, 404)

    # else, continue
    client.delete(business_key)

    # delete all associated reviews
    # query for all reviews
    query = client.query(kind=REVIEWS)
    # add filter
    query.add_filter('business_id', '=', business_id)

    # creates list of results
    results = list(query.fetch())

    # delete every review associated with this business
    for entity in results:
        client.delete(entity.key)

    return ('', 204)


"""
Reviews entity
auto-assigned (datacloud) id
user_id
business_id
stars
review_text

"""


# POST route creates a review
# verifies content
# responds 400 ERROR when content attributes are missing
# responds 404 ERROR when business is not in datastore
# responds 409 ERROR when a review has already been made for the same business
# store information in datastore, associated with user reviewing the business
@app.route('/' + REVIEWS, methods=['POST'])
def post_reviews():

    content = request.get_json()

    # if content is not valid, return 400
    if validate_review(content) is False:
        return (ERROR_MISSING, 400)

    # check for existing business
    business_id = content['business_id']
    business_key = client.key(BUSINESSES, business_id)
    business = client.get(key=business_key)
    if business is None:
        return (ERROR_NO_BUS, 404)

    # check for existing review for same business
    existing_reviews = get_reviews_for_user(content['user_id'])[0]

    for review in existing_reviews:
        if review['business_id'] == business_id:
            return (ERROR_CONFLICT, 409)

    # create datastore entity
    new_entity = datastore.Entity(key=client.key(REVIEWS))
    new_entity.update({
        'user_id': content['user_id'],
        'business_id': content['business_id'],
        'stars': content['stars']
    })

    # optional attributes
    if 'review_text' in content:
        new_entity.update({
         'review_text': content['review_text']})

    # save the new entity in datastore
    client.put(new_entity)

    # capture assigned id
    new_entity['id'] = new_entity.key.id

    # return a 201 response with newly created data
    return (jsonify(new_entity), 201)


# GET route returns information for a specific review
# Returns 404 if review is not found
@app.route('/' + REVIEWS + '/<int:review_id>', methods=['GET'])
def get_review(review_id):

    # fetch review from datastore
    review_key = client.key(REVIEWS, review_id)
    review = client.get(key=review_key)

    # if review does not exist, return error
    if review is None:
        return (ERROR_NO_REVIEW, 404)

    # add key to information to return
    review['id'] = review.key.id

    # return single review and 200 status
    return (jsonify(review), 200)


# GET route returns all reviews associated with a specific user
@app.route('/' + USERS + '/<int:user_id>/' + REVIEWS, methods=['GET'])
def get_reviews_for_user(user_id):

    # query for all reviews
    query = client.query(kind=REVIEWS)
    # add filter
    query.add_filter('user_id', '=', user_id)

    # creates list of results
    results = list(query.fetch())

    # add ids for ease of use
    for r in results:
        r['id'] = r.key.id

    return (results, 200)


# PUT route updates a review given a review_id
# returns 404 ERROR if there is no existing review
# returns 400 ERROR when stars attribute is not included in request body
@app.route('/' + REVIEWS + '/<int:review_id>', methods=['PUT'])
def put_review(review_id):

    # fetch review from datastore
    content = request.get_json()

    review_key = client.key(REVIEWS, review_id)
    review = client.get(key=review_key)

    # check that review exists
    if review is None:
        return (ERROR_NO_REVIEW, 404)

    # update review
    review['id'] = review.key.id

    if 'stars' not in content:
        return (ERROR_MISSING, 400)

    # required attributes
    review.update({
        'stars': content['stars']
        })

    # optional attributes
    if 'review_text' in content:
        review.update({
            'review_text': content['review_text']})

    # update datastore entry
    client.put(review)

    # confirm review id
    review['id'] = review.key.id

    # return updated review
    return review


# DELETE route deletes a review with a given id
# returns 404 ERROR if review does not exist
@app.route('/' + REVIEWS + '/<int:review_id>', methods=['DELETE'])
def delete_review(review_id):

    review_key = client.key(REVIEWS, review_id)
    review = client.get(key=review_key)

    if review is None:
        return (ERROR_NO_REVIEW, 404)
    else:
        client.delete(review_key)
        return ('', 204)


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
