// References to DOM elements
const form = document.getElementById('upload-form');
const resultDiv = document.getElementById('result');
const imageInput = document.getElementById('image-input');
const imagePreview = document.getElementById('image-preview');

// Show small image preview immediately after selecting file
imageInput.addEventListener('change', () => {
  const file = imageInput.files[0];
  if (file && file.type.startsWith('image/')) {
    const reader = new FileReader();
    reader.onload = () => {
      imagePreview.innerHTML = `<img src="${reader.result}" alt="Preview">`;
    };
    reader.readAsDataURL(file);
  } else {
    imagePreview.innerHTML = '';
  }
});


form.addEventListener('submit', async (e) => {
  e.preventDefault();

  const formData = new FormData(form);
  resultDiv.innerHTML = '<p>preparing recipe items..</p>';

  try {
    const response = await fetch('http://127.0.0.1:5000/extract/ingredients', {
      method: 'POST',
      body: formData
    });

    const data = await response.json();

    let total = 0;

    // Generate table rows with prices
   const rowsHtml = data.ingredients.map((item, index) => {

  const qty = Number(item.qty) || 1;
  const unitPrice = Number(item.unitPrice) || 0;
  const price = qty * unitPrice;

  total += price;

  return `
    <tr data-unitprice="${unitPrice}">
      <td>${item.itemName}</td>
      
     <td>
  <div class="qty-wrapper">
    <button type="button" class="qty-btn" onclick="changeQty(this, -1)">-</button>
    <input 
      type="number" 
      value="${qty}" 
      min="0.1" 
      step="0.1"
      class="qty-input"
      onchange="updatePrice(this)"
    />
    <button type="button" class="qty-btn" onclick="changeQty(this, 1)">+</button>
    ${item.unit}
  </div>
</td>

      <td>${item.allergen}</td>
      
      <td class="price-cell">$${price.toFixed(2)}</td>

      <td>
        <button class="remove-btn" onclick="removeItem(this)">
          Remove
        </button>
      </td>
    </tr>
  `;
}).join('');

// Render table
    resultDiv.innerHTML = `
  <h3>${data.foodName} for ${data.noOfPersons} people</h3>
  <table>
    <thead>
      <tr>
        <th>Item Name</th>
        <th>Quantity</th>
        <th>Is Allergen?</th>
        <th>Price</th>
        <th>Remove</th>
      </tr>
    </thead>
    <tbody>
      ${rowsHtml}
      <tr class="total-row">
        <td colspan="3"><strong>Total</strong></td>
        <td colspan="2" id="total-price"><strong>$${total.toFixed(2)}</strong></td>
      </tr>
    </tbody>
  </table>

  <div class="recipe-section">
    <h3>Would you like to see the recipe for this?</h3>
    <button onclick="showRecipeOptions('${data.foodName}')">
      Yes, Show Recipes
    </button>
  </div>

  <div id="recipe-options"></div>
  <div id="recipe-output"></div>
`;
  } catch (err) {
    console.error(err);
    resultDiv.innerHTML = '<p>Error fetching data</p>';
  }
});

// Remove item function
function removeItem(button) {
  const row = button.closest('tr');
  if (!row) return;

  row.remove();

  recalculateTotal();
}

// Recalculate total after removing item
function recalculateTotal() {
  const priceCells = resultDiv.querySelectorAll('.price-cell');
  let newTotal = 0;

  priceCells.forEach(cell => {
    const value = parseFloat(cell.textContent.replace('$', '')) || 0;
    newTotal += value;
  });

  const totalCell = document.getElementById('total-price');
  if (totalCell) {
    totalCell.innerHTML = `<strong>$${newTotal.toFixed(2)}</strong>`;
  }
}

function showTextSearch(button) {
  document.getElementById("text-section").classList.remove("hidden");
  document.getElementById("image-section").classList.add("hidden");

  setActive(button);
}

function showImageSearch(button) {
  document.getElementById("image-section").classList.remove("hidden");
  document.getElementById("text-section").classList.add("hidden");

  setActive(button);
}

function setActive(activeBtn) {
  document.querySelectorAll(".option-btn").forEach(btn => {
    btn.classList.remove("active");
  });

  activeBtn.classList.add("active");
}

function showRecipeOptions(foodName) {
  const optionsDiv = document.getElementById("recipe-options");

  optionsDiv.innerHTML = `
    <h4>Select Recipe Version:</h4>
    <button onclick="fetchRecipe('${foodName}', 'Home Style')">
      Home Style
    </button>
    <button onclick="fetchRecipe('${foodName}', 'Restaurant Style')">
      Restaurant Style
    </button>
    <button onclick="fetchRecipe('${foodName}', 'Healthy Version')">
      Healthy Version
    </button>
  `;
}

async function fetchRecipe(foodName, version) {

  const outputDiv = document.getElementById("recipe-output");
  outputDiv.innerHTML = "<p>Preparing recipe...</p>";

  try {
    const response = await fetch("http://127.0.0.1:5000/generate/recipe", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        foodName: foodName,
        version: version
      })
    });

    const data = await response.json();

    const stepsHtml = data.steps.map(step => 
      `<li>${step}</li>`
    ).join("");

    outputDiv.innerHTML = `
      <h3>${data.foodName} - ${data.version}</h3>
      <ol>
        ${stepsHtml}
      </ol>
    `;

  } catch (err) {
    outputDiv.innerHTML = "<p>Error generating recipe.</p>";
  }
}

// Update price when quantity changes
function updatePrice(input) {
  const row = input.closest('tr');
  if (!row) return;

  const qty = parseFloat(input.value) || 0;
  const unitPrice = parseFloat(row.getAttribute('data-unitprice')) || 0;

  const priceCell = row.querySelector('.price-cell');
  const newPrice = qty * unitPrice;
  priceCell.textContent = `$${newPrice.toFixed(2)}`;

  recalculateTotal();
}

// Change quantity via + / - buttons
function changeQty(button, delta) {
  const input = button.parentElement.querySelector('.qty-input');
  if (!input) return;

  let currentValue = parseFloat(input.value) || 0;
  let step = parseFloat(input.step) || 1;
  let min = parseFloat(input.min) || 0;

  currentValue += delta * step;
  if (currentValue < min) currentValue = min;

  input.value = currentValue.toFixed(2); // keep 2 decimals
  updatePrice(input);
}
