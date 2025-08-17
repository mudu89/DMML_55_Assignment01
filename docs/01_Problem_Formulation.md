# Task 1 — Problem Formulation (Telecom / Pay-TV Domain)

## 1. Business Problem
In the telecom/TV services industry, **customer churn** occurs when subscribers raise disconnect requests through the CRM system. This can take two forms:  

- **Full disconnect**: the customer terminates *all* subscribed services (e.g., broadband + TV + phone), ending the relationship entirely.  
- **Partial disconnect**: the customer terminates or downgrades *specific services* (e.g., drops TV but retains broadband).  

Both types lead to revenue loss — with full churn being more severe. Beyond immediate revenue impact, churn also increases **customer acquisition costs** (to replace lost users) and risks **negative word-of-mouth**, further harming the brand.  

The business problem is to **predict which customers are likely to disconnect services** so that the company can intervene proactively (e.g., targeted offers, improved service, win-back campaigns).  

---

## 2. Key Business Objectives
The churn prediction pipeline should support the following objectives:  

1. **Predict churn risk** at the *customer–product* level.  
   - Not just “will the customer churn,” but “which service is at risk of disconnect.”  

2. **Differentiate churn types** (full vs. partial) so that retention teams can prioritize interventions.  

3. **Enable proactive retention** by delivering churn risk scores early enough for the business to act (e.g., before billing cycle end).  

4. **Support strategic planning** by identifying drivers of churn (pricing, service quality, competition) and improving long-term retention strategies.  

---

## 3. Key Data Sources and Attributes  

### **A. CRM / Service Tickets** (Primary churn ground truth)  
- **ticket_id** – unique identifier of service request  
- **customer_id** – foreign key to customer master  
- **product_id** – product/service being disconnected  
- **created_at** – timestamp of request  
- **request_type** – e.g., disconnect, complaint, upgrade  
- **disconnect_reason** – e.g., competitor offer, service issues, relocation  
- **status** – open, closed (closed + disconnect = churn event)  

---

### **B. Product Subscriptions (Customer–Product Master)**  
- **customer_id** – unique subscriber  
- **product_id** – broadband, TV, OTT, etc.  
- **subscription_start / end** – tenure details  
- **plan_type** – month-to-month, annual, bundle  
- **monthly_fee** – expected charges  
- **bundle_flag** – whether part of a bundled package  
- **status** – active, inactive, churned  

---

### **C. Billing & Payments**  
- **invoice_id** – unique bill reference  
- **customer_id, product_id** – mapping to subscription  
- **invoice_date** – billing cycle  
- **amount_due / amount_paid** – billing amounts  
- **payment_date** – when paid  
- **payment_method** – UPI, card, bank transfer  
- **payment_status** – paid, pending, defaulted  

---

### **D. Usage Logs (Optional, Derived or Synthetic)**  
- **customer_id, product_id** – subscriber mapping  
- **event_timestamp** – when activity occurred  
- **event_type** – call, data, TV view, OTT login  
- **usage_volume** – e.g., GB consumed, minutes watched  
- **channel** – mobile app, set-top box, web  
