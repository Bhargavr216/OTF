import google.generativeai as genai

from PIL import Image

import io
from flask import Flask,request
from flask_cors import CORS


app = Flask(__name__)
# Configure Gemini API
CORS(app)
# genai.configure(api_key='AIzaSyBjSsg5RJVWvgVMVMdxizxvg1tgHSrSSbI')
genai.configure(api_key='AIzaSyABkpJuufZEOQB81TClObvnh4H-ePP39v0')

@app.route('/extract/ingredients', methods=['POST'])
def extract_ingredients_qty():



    """
    start flask server
    open new terminal
    python -m http.server 8000
    Sample output able to produce:
    Ingredients: {"foodName": "Paneer 65", "ingredients": [{itemName:"Paneer",qty:"250 gm"}, {itemName:"Cornflour",qty:"3 tbsp"}, {itemName:"Rice Flour",qty:"2 tbsp"}, {itemName:"Ginger Garlic Paste",qty:"1 tbsp"}, {itemName:"Red Chili Powder",qty:"1.5 tsp"}, {itemName:"Turmeric Powder",qty:"0.5 tsp"}, {itemName:"Curry Leaves",qty:"10-12 leaves"}, {itemName:"Green Chilies",qty:"2"}, {itemName:"Salt",qty:"to taste"}, {itemName:"Cooking Oil",qty:"for deep frying"}, {itemName:"Lemon Juice",qty:"1 tbsp"}, {itemName:"Coriander Leaves",qty:"2 tbsp"}, {itemName:"Cashew Nuts",qty:"10-12"}]}

    or

    {"itemName": "Not a food item", "ingredients":}
    """
    # Load image and identify food item

    model = genai.GenerativeModel('gemini-2.5-flash')
    image_path = request.files['image']
    NoOfPersons = request.form.get('persons')
    dishName = request.form.get('dishName')
    #image_path = 'dmla.jpg'  

    image = Image.open(image_path)
    image_byte_arr = io.BytesIO()
    image.save(image_byte_arr, format='JPEG')
    image_blob = image_byte_arr.getvalue()

    SYSTEM_PROMPT = SYSTEM_PROMPT = """
You are an expert food recognition and ingredient estimation system.

Return STRICT JSON in this format:

{
  "foodName": "string",
  "noOfPersons": number,
  "ingredients": [
    {
      "itemName": "string",
      "qty": number,
      "unit": "string",
      "allergen": boolean,
      "countryOfOrigin": "string",
      "unitPrice": number
    }
  ]
}

Rules:
- qty must be numeric
- unitPrice must be numeric (USD price per unit)
- Do NOT return text
- Do NOT wrap in markdown
- Always include unitPrice
"""

    USER_PROMPT = f"What's this food item {dishName} and its ingredients with their respective quantities and allergen information for {NoOfPersons} persons?"

    messages = [ 
        {"role": "model", "parts": [{"text": SYSTEM_PROMPT}]},
        {"role": "user", "parts": [
            {"text": USER_PROMPT}, 
            {"inline_data": {"mime_type": "image/jpeg", "data": image_blob}}
            ]}
    ]
    

    ingredients_qty_response = model.generate_content(messages)

    ingredients_qty = ingredients_qty_response.text

    print('Ingredients and Quantity:', ingredients_qty)
    return ingredients_qty

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)


# python -m venv venv

# venv\Scripts\activate

# python app.py
 
